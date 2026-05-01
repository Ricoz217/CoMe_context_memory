"""
LLM_connect.py
LLM通信模块，面向对象设计，封装输入输出接口，简化API调用过程
"""

from __future__ import annotations
__version__ = "0.1.6"

import copy
import json
import inspect
import types
import base64
import asyncio
import time
import httpx
import datetime
import traceback
import tiktoken
from enum import Enum
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Awaitable, Any, Annotated, Literal, Union, get_args, get_origin, get_type_hints, \
    TYPE_CHECKING
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from come_context_memory.time_id import next_time_id
from come_context_memory.logger import get_logger
from come_context_memory.utils import resize_image_to_base64, parse_proxies_to_httpx, AutoMapping
from come_context_memory.file_cache import add_file, get_file_path
from come_context_memory.LLM_usage import ApiPrice, LLMUsage, _GLOBAL_USAGE
from come_context_memory.config import SETTING_CFG, get_llm, get_proxy, DATA_DIR

if TYPE_CHECKING:
    from come_context_memory.logger import BlockHandle, Logger


_log = get_logger()
_MAPPING_FILE = DATA_DIR / "llm_connect" / "image_name_mapping.json"
_IMAGE_NAME_MAPPING: AutoMapping[list[str]] = AutoMapping(_MAPPING_FILE, expire_day=14)
_UNION_TYPE = getattr(types, "UnionType", None)


async def handle_unload_image(image: str | Path | bytes, cache: dict) -> list[str]:
    if isinstance(image, str):
        image = base64.b64decode(image)

    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(executor, resize_image_to_base64, image)

        except Exception as E:
            _log.error(E)
            return []

        else:
            b64_list = []
            for b64_str in result:
                hash_name = add_file(b64_str, "IMAGE")
                cache[hash_name] = b64_str
                b64_list.append(hash_name)

            return b64_list

def build_tool_payloads(func: Any) -> dict[str, dict[str, Any]]:
    """Build OpenAI/Anthropic tool payloads from a Python callable.

    Works for regular functions and ``async def`` coroutine functions. For tool
    schema generation, only input parameters are used.
    """
    if not callable(func):
        raise TypeError("func must be callable")

    def _annotation_to_schema(_annotation: Any) -> dict[str, Any]:
        origin = get_origin(_annotation)

        if _annotation is Any:
            return {}

        if _annotation is type(None):
            return {"type": "null"}

        if origin is not None:
            if origin is Annotated:
                base, *meta = get_args(_annotation)
                _schema = _annotation_to_schema(base)
                for item in meta:
                    if isinstance(item, str) and item.strip():
                        _schema = {**_schema, "description": item.strip()}
                        break
                return _schema

            if (_UNION_TYPE is not None and origin is _UNION_TYPE) or origin is Union:
                return _union_schema(get_args(_annotation))

            if origin in (list, tuple, set, frozenset):
                args = get_args(_annotation)
                item_schema = _annotation_to_schema(args[0]) if args else {}
                return {"type": "array", "items": item_schema}

            if origin is dict:
                args = get_args(_annotation)
                value_schema = _annotation_to_schema(args[1]) if len(args) == 2 else {}
                return {"type": "object", "additionalProperties": value_schema}

            if origin is Literal:
                values = list(get_args(_annotation))
                if not values:
                    return {}
                literal_types = {type(v) for v in values}
                if len(literal_types) == 1:
                    mapped = _python_type_to_json_type(next(iter(literal_types)))
                    return {"type": mapped, "enum": values}
                return {"enum": values}

        if inspect.isclass(_annotation) and issubclass(_annotation, Enum):
            values = [member.value for member in _annotation]
            enum_type = _python_type_to_json_type(type(values[0])) if values else "string"
            return {"type": enum_type, "enum": values}

        mapped = _python_type_to_json_type(_annotation)
        if mapped:
            return {"type": mapped}

        return {}

    def _union_schema(_args: tuple[Any, ...]) -> dict[str, Any]:
        _schema = [_annotation_to_schema(arg) for arg in _args]

        # Compact Optional[X] into type union when possible.
        if len(_schema) == 2 and any(s.get("type") == "null" for s in _schema):
            non_null = next((s for s in _schema if s.get("type") != "null"), None)
            if non_null and isinstance(non_null.get("type"), str):
                merged = dict(non_null)
                merged["type"] = [non_null["type"], "null"]
                return merged

        return {"anyOf": _schema}

    def _python_type_to_json_type(_annotation: Any) -> str | None:
        mapping: dict[Any, str] = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            dict: "object",
            list: "array",
            tuple: "array",
            set: "array",
            frozenset: "array",
            type(None): "null",
        }
        return mapping.get(_annotation)

    def _json_safe_default(_value: Any) -> Any:
        if isinstance(_value, Enum):
            return _value.value
        return _value

    def _return_annotation_to_schema(_annotation: Any) -> dict[str, Any] | None:
        if _annotation in (inspect.Signature.empty, Any):
            return None
        _schema = _annotation_to_schema(_annotation)
        return _schema or None

    def _parse_doc_sections(_doc: str) -> tuple[str, dict[str, str], str]:
        """
        Parse reST-style docstring blocks and split into:
        - global description text
        - per-parameter descriptions from :param
        - return description from :return / :returns
        """
        general_parts: list[str] = []
        param_map: dict[str, list[str]] = {}
        return_parts: list[str] = []

        lines = _doc.splitlines()
        idx = 0
        current_param: str | None = None
        in_return_block = False
        while idx < len(lines):
            stripped = lines[idx].strip()
            if not stripped:
                idx += 1
                continue

            if stripped.startswith(":param "):
                current_param = None
                in_return_block = False

                body = stripped[len(":param "):]
                if ":" in body:
                    left, first_desc = body.split(":", 1)
                    left = left.strip()
                    param_name = left.split()[-1] if left else ""
                    if param_name:
                        param_map.setdefault(param_name, [])
                        if first_desc.strip():
                            param_map[param_name].append(first_desc.strip())
                        current_param = param_name

                idx += 1
                continue

            if stripped.startswith(":return:") or stripped.startswith(":returns:"):
                current_param = None
                in_return_block = True
                first_desc = stripped.split(":", 2)[-1].strip()
                if first_desc:
                    return_parts.append(first_desc)
                idx += 1
                continue

            if stripped.startswith(":rtype:"):
                current_param = None
                in_return_block = False
                idx += 1
                continue

            # New directive starts; end current param/return capture.
            if stripped.startswith(":") and ":" in stripped[1:]:
                current_param = None
                in_return_block = False
                idx += 1
                continue

            if current_param is not None:
                param_map[current_param].append(stripped)
            elif in_return_block:
                return_parts.append(stripped)
            else:
                general_parts.append(stripped)

            idx += 1

        param_descriptions = {
            key: " ".join(parts).strip() for key, parts in param_map.items() if parts
        }
        return_description = " ".join(return_parts).strip()
        general_description = " ".join(general_parts).strip()
        return general_description, param_descriptions, return_description

    def _schema_to_text(_schema: dict[str, Any]) -> str:
        if not _schema:
            return "any"

        if "anyOf" in _schema:
            return " | ".join(_schema_to_text(part) for part in _schema["anyOf"])

        value_type = _schema.get("type")
        if isinstance(value_type, list):
            return " | ".join(str(item) for item in value_type)
        if value_type == "array":
            return f"array<{_schema_to_text(_schema.get('items', {}))}>"
        if value_type == "object" and "additionalProperties" in _schema:
            return f"object<string, {_schema_to_text(_schema['additionalProperties'])}>"
        if "enum" in _schema:
            return f"{value_type or 'enum'} ({', '.join(map(str, _schema['enum']))})"
        if isinstance(value_type, str):
            return value_type
        return "any"

    # This works for both regular functions and async coroutine functions.
    signature = inspect.signature(func)
    hints = get_type_hints(func, include_extras=True)

    doc = inspect.getdoc(func) or ""
    doc_description, doc_param_descriptions, doc_return_description = _parse_doc_sections(doc)

    properties: dict[str, dict[str, Any]] = {}
    required: list[str] = []

    for name, param in signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError("Tool schema generation does not support *args or **kwargs")

        annotation = hints.get(name, Any)
        schema = _annotation_to_schema(annotation)
        doc_param_desc = doc_param_descriptions.get(name, "")
        if doc_param_desc:
            existing_desc = schema.get("description", "").strip() if isinstance(schema.get("description"), str) else ""
            if existing_desc:
                schema["description"] = f"{existing_desc} {doc_param_desc}"
            else:
                schema["description"] = doc_param_desc

        if param.default is inspect.Signature.empty:
            required.append(name)
        else:
            schema["default"] = _json_safe_default(param.default)

        properties[name] = schema

    description = doc_description if doc_description else f"Tool generated from `{func.__name__}`"
    return_annotation = hints.get("return", signature.return_annotation)
    return_schema = _return_annotation_to_schema(return_annotation)
    return_summary = ""
    if return_schema:
        return_summary = _schema_to_text(return_schema)
    if doc_return_description:
        if return_summary:
            return_summary = f"{return_summary}. {doc_return_description}"
        else:
            return_summary = doc_return_description
    if return_summary:
        description = f"{description} Returns: {return_summary}."

    params_schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        params_schema["required"] = required

    openai = {
        "type": "function",
        "function": {
            "name": func.__name__,
            "description": description,
            "parameters": params_schema,
        },
    }
    if return_schema:
        openai["function"]["returns"] = return_schema

    anthropic = {
        "name": func.__name__,
        "description": description,
        "input_schema": params_schema,
    }
    if return_schema:
        anthropic["returns"] = return_schema

    return {"openai": openai, "anthropic": anthropic}


class ContextOverflowError(Exception):
    """超出模型context窗口错误"""
    pass


class ContextEmptyError(Exception):
    pass


class PromptType(Enum):
    BASE = "BASE"
    SYSTEM = "SYSTEM"
    TEXT = "TEXT"
    IMAGE = "IMAGE"
    TOOL_IN = "TOOL_IN"
    TOOL_CALL = "TOOLCALL"
    TOOL_RESP = "TOOL_RESP"
    TOKEN = "TOKEN"


class BasePrompt:
    def __init__(
            self,
            role: str,
            contain_type: str,
            create_time: str = None
    ):
        if create_time is None:
            create_time = str(time.time())

        self.role: str = role
        self.contain_type: str = contain_type
        self.time: str = create_time
        self.id: str = str(next_time_id())

    def to_dict(self) -> dict:
        return {
            "type": PromptType.BASE.value,
            "role": self.role,
            "contain_type": self.contain_type,
            "time": self.time,
            "id": self.id
        }

    def __eq__(self, other):
        if isinstance(other, BasePrompt):
            return self.role == other.role and self.contain_type == other.contain_type

        else:
            return False

    def __str__(self):
        dt = datetime.datetime.fromtimestamp(float(self.time))
        output = f"[{self.role}] {dt:%Y-%m-%d %H:%M:%S}: "
        return output

    def __add__(self, other):
        return Prompts(self, other)


class SystemPrompt(BasePrompt):
    def __init__(self, text: str, create_time: str = None):
        super().__init__("system", "TEXT", create_time)
        self.text = text

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["type"] = PromptType.SYSTEM.value
        base_dict["data"] = {
            "text": self.text
        }
        return base_dict

    def copy(self):
        return self.__copy__()

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = SystemPrompt(data["data"]["text"], data["time"])
        new_prompt.id = data["id"]
        return new_prompt

    def __copy__(self):
        return SystemPrompt(
            self.text,
            create_time=self.time
        )

    def __eq__(self, other):
        if isinstance(other, SystemPrompt):
            return super().__eq__(other) and self.text == other.text

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += self.text
        output += '\n'
        return output

    def __bool__(self):
        return bool(self.text)


class TextPrompt(BasePrompt):
    def __init__(self, role: str, text: str, reasoning_content: str = "", create_time: str = None):
        super().__init__(role, "TEXT", create_time)
        self.text: str = text
        self.reasoning_content: str = reasoning_content

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["type"] = PromptType.TEXT.value
        base_dict["data"] = {
            "text": self.text,
            "reasoning_content": self.reasoning_content
        }
        return base_dict

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = TextPrompt(
            role=data["role"],
            text=data["data"].get("text", ""),
            reasoning_content=data["data"].get("reasoning_content", ""),
            create_time=data["time"]
        )
        new_prompt.id = data["id"]
        return new_prompt

    def __eq__(self, other):
        if isinstance(other, TextPrompt):
            return super().__eq__(other) and self.text == other.text and self.reasoning_content == other.reasoning_content

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += self.text
        output += '\n'
        return output

    def __bool__(self):
        return bool(self.text)


class ImagePrompt(BasePrompt):
    def __init__(self, role: str, image: str | Path | bytes, name: str = "", create_time: str = None):
        super().__init__(role, "IMAGE", create_time)
        self.name: str = name  # 图片的唯一ID，用于查询缓存
        self.image: list[str] = []  # base64字符串哈希列表，节省内存
        self.image_input = image

    def to_dict(self) -> dict:
        if not isinstance(self.image_input, Path):
            origin_image_path = get_file_path(add_file(self.image_input))

        else:
            origin_image_path = self.image_input

        base_dict = super().to_dict()
        base_dict["type"] = PromptType.IMAGE.value
        base_dict["data"] = {
            "name": self.name,
            "image": self.image,
            "image_input": str(origin_image_path)
        }
        return base_dict

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = ImagePrompt(data["role"], Path(data["data"]["image_input"]), data["data"]["name"], data["time"])
        new_prompt.id = data["id"]
        new_prompt.image = data["data"]["image"]
        return new_prompt

    async def load_image(self, cache: dict):
        if self.name and self.name in _IMAGE_NAME_MAPPING:
            hash_name_list = _IMAGE_NAME_MAPPING[self.name]

        else:
            hash_name = add_file(self.image_input)
            hash_name_list = await handle_unload_image(self.image_input, cache)
            self.image_input = get_file_path(hash_name)
            if hash_name_list and self.name:
                _IMAGE_NAME_MAPPING[self.name] = hash_name_list

        self.image = hash_name_list

    def __eq__(self, other):
        if isinstance(other, ImagePrompt):
            return bool(self.image) and super().__eq__(other) and self.name == other.name and self.image == other.image

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += f"[图片;hash_id: {self.image}, name: {self.name}]"
        output += '\n'
        return output


class ToolInput(BasePrompt):
    def __init__(self, function: Callable | Awaitable, function_name: str = None, create_time: str = None):
        if function_name is None:
            function_name = function.__name__

        super().__init__("function", "FUNCTION", create_time)
        self.function: Callable | Awaitable = function
        self.function_name: str = function_name
        if inspect.iscoroutinefunction(function):
            self.function_type = "ASYNC"

        else:
            self.function_type = "SYNC"

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["type"] = PromptType.TOOL_IN.value
        base_dict["data"] = {
            "function_name": self.function_name,
            "function_type": self.function_type
        }
        return base_dict

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = ToolInput(data["data"]["function"], data["data"]["function_name"], data["time"])
        new_prompt.id = data["id"]
        return new_prompt

    def __eq__(self, other):
        if isinstance(other, ToolInput):
            return super().__eq__(other) and self.function is other.function and self.function_type == other.function_type

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += f"[TOOL输入; function_name: {self.function_name}]"
        output += '\n'
        return output


class ToolCall(BasePrompt):
    def __init__(self, function_name: str, call_id: str, arguments: Any):
        super().__init__("assistant", "FUNCTION")
        self.function_name: str = function_name
        self.call_id: str = call_id
        self.arguments: Any = arguments

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["type"] = PromptType.TOOL_CALL.value
        base_dict["data"] = {
            "function_name": self.function_name,
            "call_id": self.call_id,
            "arguments": self.arguments
        }
        return base_dict

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = ToolCall(data["data"]["function_name"], data["data"]["call_id"], data["data"]["arguments"])
        new_prompt.id = data["id"]
        new_prompt.time = data["time"]
        return new_prompt

    def __eq__(self, other):
        if isinstance(other, ToolCall):
            return super().__eq__(
                other) and self.function_name == other.function_name and self.call_id == other.call_id and self.arguments == other.arguments

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += f"[TOOL CALL; function_name: {self.function_name}, arguments: {self.arguments}]"
        output += '\n'
        return output


class ToolResponse(BasePrompt):
    def __init__(self, function_name: str, call_id: str, response: Any, create_time: str = None):
        super().__init__("tool", "FUNCTION", create_time)
        self.function_name = function_name
        self.call_id: str = call_id
        self.response = response

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["type"] = PromptType.TOOL_RESP.value
        base_dict["data"] = {
            "function_name": self.function_name,
            "call_id": self.call_id,
            "response": self.response
        }
        return base_dict

    @classmethod
    def from_dict(cls, data: dict):
        new_prompt = ToolResponse(data["data"]["function_name"], data["data"]["call_id"], data["data"]["response"], data["time"])
        new_prompt.id = data["id"]
        return new_prompt

    def __eq__(self, other):
        if isinstance(other, ToolResponse):
            return super().__eq__(
                other) and self.function_name == other.function_name and self.call_id == other.call_id and self.response == other.response

        else:
            return False

    def __str__(self):
        output = super().__str__()
        output += f"[TOOL RESPONSE; function_name: {self.function_name}, response: {self.response}]"
        output += '\n'
        return output


@dataclass(slots=True)
class TokenUsage:
    """统一 token 统计结构。"""
    input_t: int = 0
    output_t: int = 0
    cache_hit_t: int = 0
    total_t: int = 0
    model: str = ""
    id: str = field(default_factory=lambda :str(next_time_id()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_t": self.input_t,
            "output_t": self.output_t,
            "cache_hit_t": self.cache_hit_t,
            "total_t": self.total_t,
            "model": self.model,
            "id": self.id
        }

    @classmethod
    def from_dict(cls, data: dict):
        new_usage = TokenUsage(
            data.get("input_t", 0),
            data.get("output_t", 0),
            data.get("cache_hit_t", 0),
            data.get("total_t", 0),
            data.get("model", ""),
            data.get("id", str(next_time_id()))
        )
        return new_usage

    def clear(self):
        self.input_t = 0
        self.output_t = 0
        self.cache_hit_t = 0
        self.total_t = 0
        self.model = ""

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return TokenUsage(
            self.input_t,
            self.output_t,
            self.cache_hit_t,
            self.total_t,
            self.model
        )

    def __add__(self, other) -> TokenUsage:
        if not isinstance(other, TokenUsage):
            raise TypeError(f"'+' doesn't support type: {type(other)}")

        if self.model != other.model:
            if self.model:
                raise ValueError("Not Same Model")

            else:
                self.model = other.model

        new_input_t = self.input_t + other.input_t
        new_output_t = self.output_t + other.output_t
        new_cache_hit_t = self.cache_hit_t + other.cache_hit_t
        new_total_t = self.total_t + other.total_t
        return TokenUsage(
            new_input_t,
            new_output_t,
            new_cache_hit_t,
            new_total_t,
            self.model
        )

    def __sub__(self, other) -> TokenUsage:
        if not isinstance(other, TokenUsage):
            raise TypeError(f"'-' doesn't support type: {type(other)}")

        if self.model != other.model:
            if self.model:
                raise ValueError("Not Same Model")

        new_input_t = max(0, self.input_t - other.input_t)
        new_output_t = max(0, self.output_t - other.output_t)
        new_cache_hit_t = max(0, self.cache_hit_t - other.cache_hit_t)
        new_total_t = max(0, self.total_t - other.total_t)
        return TokenUsage(
            new_input_t,
            new_output_t,
            new_cache_hit_t,
            new_total_t,
            self.model
        )

    def __gt__(self, other):
        if not isinstance(other, TokenUsage):
            raise TypeError(f"'>' doesn't support type: {type(other)}")

        if self.model != other.model:
            if self.model:
                raise ValueError("Not Same Model")

        return self.total_t > other.total_t

    def __lt__(self, other):
        if not isinstance(other, TokenUsage):
            raise TypeError(f"'>' doesn't support type: {type(other)}")

        if self.model != other.model:
            if self.model:
                raise ValueError("Not Same Model")

        return self.total_t < other.total_t

    def __bool__(self):
        return bool(self.total_t)


class Prompts:
    def __init__(self, *args, role: str = "", create_time: str = None, duration: float = 0.0):
        if create_time is None:
            create_time = time.time()

        self.prompts: list[BasePrompt] = []
        self.role = role
        self.time = create_time  # 最新一条消息的时间
        self._duration: float = duration  # 耗时
        self.usage: TokenUsage | None = None
        if args:
            self._initiate_prompts(args)

    def _add_usage(self, usage: TokenUsage):
        if self.usage is None:
            self.usage = usage

        else:
            self.usage += usage

    def _unpack_prompts(self, prompts, roles: set):
        if isinstance(prompts, Prompts):
            for p in prompts.prompts:
                roles.add(p.role)
                self.prompts.append(p)

            if prompts.usage is not None:
                self._add_usage(prompts.usage)

            self.time = prompts.time

        elif isinstance(prompts, (list, tuple)):
            for p in prompts:
                self._unpack_prompts(p, roles)

        elif isinstance(prompts, BasePrompt):
            roles.add(prompts.role)
            self.prompts.append(prompts)
            self.time = prompts.time

        elif isinstance(prompts, str):  # 仅提供对文本内容的快捷转换
            self._text2prompt(prompts, roles)

        elif isinstance(prompts, TokenUsage):
            self._add_usage(prompts)

        else:
            raise TypeError(f"不支持{type(prompts)}生成Prompts")

    def _initiate_prompts(self, *args):
        roles = set()
        for prompts in args:
            self._unpack_prompts(prompts, roles)

        if len(roles) == 1:
            self.role = next(iter(roles))

        elif len(roles) > 1:
            self.role = "MIX"

    def _text2prompt(self, text: str, roles: set):
        if len(roles) == 1:
            role = next(iter(roles))

        elif self.role:
            role = self.role

        else:
            raise ValueError("需要指定一个role")

        self.prompts.append(TextPrompt(role, text))

    def to_dict(self) -> dict:
        prompts = [prompt.to_dict() for prompt in self.prompts]
        usage = self.usage.to_dict() if self.usage is not None else {}
        return {
            "prompts": prompts,
            "role": self.role,
            "time": self.time,
            "duration": self._duration,
            "usage": usage
        }

    @property
    def duration(self) -> float:
        if self._duration:
            return self._duration

        else:
            if len(self.prompts) > 1:
                return max(float(self.prompts[-1].time) - float(self.prompts[0].time), 0.0)

            else:
                raise 0.0

    def append(self, addition: BasePrompt | Prompts | TokenUsage | str | list) -> None:
        roles = set()
        self._unpack_prompts(addition, roles)
        if len(roles) == 1:
            new_role = next(iter(roles))
            if self.role:
                if self.role != new_role:
                    self.role = "MIX"

            else:
                self.role = new_role

        else:
            self.role = "MIX"

    def extend(self, addition: BasePrompt | Prompts | str | list) -> None:
        return self.append(addition)

    def copy(self):
        return self.__copy__()

    def __str__(self):
        output = ""
        for prompt in self.prompts:
            output += str(prompt)

        return output

    def __contains__(self, item):
        if isinstance(item, BasePrompt):
            for p in self.prompts:
                if p == item:
                    return True

            return False

        else:
            return False

    def __iter__(self):
        return iter(self.prompts)

    def __reversed__(self):
        return reversed(self.prompts)

    def __add__(self, other):
        if isinstance(other, (Prompts, BasePrompt, str)):
            return Prompts(self, other)

        else:
            raise TypeError(f"Prompts add doesn't support {type(other)}")

    def __sub__(self, other):
        origin = self.prompts.copy()
        if isinstance(other, Prompts):
            sub = other.prompts.copy()
            for p in origin[:]:
                if p in sub:
                    origin.remove(p)

            if self.usage and other.usage:
                new_usage = self.usage - other.usage
                return Prompts(origin, new_usage)

            return Prompts(origin)

        elif isinstance(other, BasePrompt):
            for p in origin[:]:
                if p == other:
                    origin.remove(p)
                    return Prompts(origin)

            return Prompts(origin)

        elif isinstance(other, TokenUsage):
            if self.usage is not None:
                new_prompts = self.copy()
                new_usage = self.usage - other
                new_prompts.usage = new_usage
                return new_prompts

            raise ValueError(f"host Prompts doesn't has TokenUsage yet")

        else:
            raise TypeError(f"Prompts sub doesn't support {type(other)}")

    def __bool__(self):
        return bool(self.prompts)

    def __copy__(self):
        copy_prompts = Prompts()
        copy_prompts.prompts = self.prompts.copy()
        copy_prompts.role = self.role
        copy_prompts.time = self.time
        copy_prompts.usage = self.usage.copy() if self.usage else None
        return copy_prompts


class Context:
    def __init__(
            self,
            *args
    ):
        self._system_prompt: SystemPrompt | None = None
        self._messages: list[BasePrompt] = []
        self._rounds: dict[str, Prompts] = {}
        self._tools: list[ToolInput] = []
        self._usage_accumulation: TokenUsage = TokenUsage()
        if args:
            self._initiate(args)

    def _initiate(self, args: tuple):
        new_round: dict[str, list] = {"prompts": [], "usage": []}
        self._unpack_prompts(args, new_round)
        if new_round:
            self._submit_round(new_round)

    def _add_usage(self, usage: TokenUsage):
        if not self._usage_accumulation.model:
            self._usage_accumulation.model = usage.model

        self._usage_accumulation += usage

    def _submit_round(self, round_list: dict):
        save_round = Prompts(round_list["prompts"], round_list["usage"])
        save_round.role = "ROUND"
        self._rounds[save_round.time] = save_round
        round_list.clear()
        round_list.update({"prompts": [], "usage": []})

    def _unpack_prompts(self, prompt, round_list: dict[str, list]):
        if isinstance(prompt, (list, tuple)):
            for p in prompt:
                self._unpack_prompts(p, round_list)

        elif isinstance(prompt, Prompts):
            temp_list: list[BasePrompt | TokenUsage] = prompt.prompts.copy()
            if prompt.usage is not None:
                temp_list.append(prompt.usage)

            self._unpack_prompts(temp_list, round_list)

        elif isinstance(prompt, SystemPrompt):
            self._system_prompt = prompt
            if self._messages and self._messages[0].role == "system":
                self._messages[0] = prompt

            else:
                self._messages.insert(0, prompt)

        elif isinstance(prompt, ToolInput):
            if prompt not in self._tools:
                self._tools.append(prompt)

        elif isinstance(prompt, BasePrompt):
            self._messages.append(prompt)
            if prompt.role != "assistant":
                if round_list["prompts"] and round_list["prompts"][-1].role == "assistant":
                    self._submit_round(round_list)

            round_list["prompts"].append(prompt)

        elif isinstance(prompt, TokenUsage):
            self._add_usage(prompt)
            round_list["usage"].append(prompt)

        else:
            raise TypeError(f"不支持{type(prompt)}类型创建Context")

    def append(self, addition: Prompts | list | BasePrompt) -> None:
        if not addition:
            return

        if self._rounds:
            key = next(reversed(self._rounds))
            last_round = {"prompts": self._rounds[key].prompts.copy(), "usage": []}
            del self._rounds[key]

        else:
            last_round = {"prompts": [], "usage": []}

        self._unpack_prompts(addition, last_round)
        if last_round.get("prompts", []):
            self._submit_round(last_round)

    def extend(self, addition: Prompts | list | BasePrompt) -> None:
        return self.append(addition)

    def copy(self):
        return self.__copy__()

    def clear(self, system: bool = False, tools: bool = False):
        self._usage_accumulation.clear()
        self._rounds.clear()
        self._messages.clear()

        if system:
            self._system_prompt = None

        else:
            if self._system_prompt is not None:
                self._messages.append(self._system_prompt)

        if tools:
            self._tools.clear()

    def to_prompts(self) -> Prompts:
        return Prompts(self._messages, self._tools)

    def back2last_round(self) -> Prompts:
        """回滚到上一轮对话，POP当前轮"""
        if not self._rounds:
            raise ContextEmptyError("已无上一轮")

        last_round = self._rounds.pop(next(reversed(self._rounds)))
        self._messages.clear()
        if self._rounds:
            for prompts in self._rounds.values():
                for prompt in prompts:
                    self._messages.append(prompt)

        return last_round

    def to_dict(self):
        messages = {prompt.id: prompt.to_dict() for prompt in self._messages}
        tools = {prompt.id: prompt.to_dict() for prompt in self._tools}
        system = self._system_prompt.id if self._system_prompt is not None else ""
        usage = self._usage_accumulation.to_dict()
        rounds = {}
        for r, data in self._rounds.items():
            round_data = {
                "messages": [p.id for p in data.prompts],
                "usage": data.usage.to_dict() if data.usage is not None else {}
            }
            rounds[r] = round_data

        dump_content = {
            "system": system,
            "messages": messages,
            "round": rounds,
            "tools": tools,
            "usage": usage
        }
        return dump_content

    @classmethod
    def from_dict(cls, data: dict, function_mapping: dict):
        new_context = Context()
        system = data.get("system", "")
        messages = data.get("messages", {})
        rounds = data.get("round", {})
        tools = data.get("tools", {})
        usage = data.get("usage", {})

        parsed_prompts = {}
        parsed_tools = {}
        parsed_rounds = {}
        # 获取完整prompt对象
        for mid, message in messages.items():
            if PromptType(message["type"]) is PromptType.SYSTEM:
                parsed_prompts[mid] = SystemPrompt.from_dict(message)

            elif PromptType(message["type"]) is PromptType.TEXT:
                parsed_prompts[mid] = TextPrompt.from_dict(message)

            elif PromptType(message["type"]) is PromptType.IMAGE:
                parsed_prompts[mid] = ImagePrompt.from_dict(message)

            elif PromptType(message["type"]) is PromptType.TOOL_CALL:
                parsed_prompts[mid] = ToolCall.from_dict(message)

            elif PromptType(message["type"]) is PromptType.TOOL_RESP:
                parsed_prompts[mid] = ToolResponse.from_dict(message)

        # 获取tool对象
        for tid, tool in tools.items():
            if PromptType(tool["type"]) is not PromptType.TOOL_IN:
                continue

            function_name = tool["data"]["function_name"]
            function = function_mapping.get(function_name, None)
            if function is None:
                continue

            tool["data"]["function"] = function
            parsed_tools[tid] = ToolInput.from_dict(tool)

        # 重构rounds
        for r, data in rounds.items():
            round_prompts = Prompts()
            for mid in data["messages"]:
                if mid not in parsed_prompts:
                    continue

                round_prompts.append(parsed_prompts[mid])

            if data["usage"]:
                round_prompts.usage = TokenUsage.from_dict(data["usage"])

            if round_prompts:
                parsed_rounds[r] = round_prompts

        new_context._messages.extend(list(parsed_prompts.values()))
        new_context._rounds.update(parsed_rounds)
        new_context._tools.extend(list(parsed_tools.values()))
        if system and system in parsed_prompts:
            new_context._system_prompt = parsed_prompts[system]

        if usage:
            new_context._usage_accumulation = TokenUsage.from_dict(usage)

        return new_context

    @property
    def last_round(self) -> Prompts:
        if not self._rounds:
            return Prompts(role="ROUND")

        last_key = next(reversed(self._rounds))
        last_round = self._rounds[last_key]
        return last_round.copy()

    @property
    def tools(self) -> Prompts:
        """
        返回输入工具列表
        :return:
        """
        return Prompts(self._tools)

    @property
    def usage(self) -> TokenUsage | None:
        """返回累积token消耗"""
        return self._usage_accumulation.copy()

    @property
    def system(self) -> SystemPrompt | None:
        if self._system_prompt is None:
            return None

        if not self._system_prompt.text:
            return None

        return self._system_prompt.copy()

    @property
    def has_content(self) -> bool:
        if self._system_prompt is not None:
            return len(self._messages) > 1

        else:
            return bool(self._messages)

    def __str__(self):
        output = ""
        for prompt in self._messages:
            output += str(prompt)

        for prompt in self._tools:
            output += str(prompt)

        return output

    def __copy__(self):
        copy_context = Context()
        copy_context._system_prompt = self._system_prompt
        copy_context._messages = self._messages.copy()
        copy_context._rounds = self._rounds.copy()
        copy_context._tools = self._tools.copy()
        copy_context._usage_accumulation = self._usage_accumulation.copy()
        return copy_context

    def __iter__(self):
        iter_list = self._messages + self._tools
        return iter(iter_list)

    def __reversed__(self):
        iter_list = self._messages + self._tools
        return reversed(iter_list)

    def __bool__(self):
        return bool(self._messages)


@dataclass(slots=True)
class AskTask:
    """
    装饰请求任务
    """
    prompts: Prompts
    timeout: float
    id: str = field(default_factory=lambda : str(time.time_ns()))
    event: asyncio.Event = field(default_factory=asyncio.Event)
    response: None | Prompts = None
    handle: asyncio.Task | None = None


@dataclass(slots=True)
class ChatConfig:
    endpoint: str = None
    token: str = None
    model: str = None
    max_context: int = None
    auto_compress_rate: float = None
    system_prompt: str = None
    api_provider: str = None
    price: ApiPrice = None
    client_params: dict = None
    model_params: dict = None
    keep_alive: bool = False
    logger: Logger | BlockHandle = None


class Chat:
    def __init__(
            self,
            *,
            endpoint: str = "",
            token: str = "",
            model: str = "",
            max_context: int = None,
            auto_compress_rate: float = None,
            system_prompt: str = "",
            api_provider: str = "openai",  # "openai" | "anthropic"
            price: ApiPrice = None,  # 模型单价, price / 1M token
            client_params: dict = None,
            model_params: dict = None,
            context: Context = None,
            keep_alive: bool = False,
            logger: Logger | BlockHandle = None,
            usage_record: list[LLMUsage] = None
    ):
        if context is None:
            context = Context()
            if system_prompt:
                context.append(SystemPrompt(system_prompt))

        if logger is None:
            logger = _log

        if client_params is None:
            client_params = {}

        if model_params is None:
            model_params = {
                "max_tokens": 8192,
                "temperature": 0.7,
                "stream": True
            }

        else:
            base_params = {
                "max_tokens": 8192,
                "temperature": 0.7,
                "stream": True
            }
            base_params.update(model_params)
            model_params = base_params

        if usage_record is None:
            usage_record = []

        # 配置参数
        self._endpoint: str = endpoint
        self._token: str = token
        self._model: str = model
        self._max_context: int | None = max_context
        self._compress_rate: float | None = auto_compress_rate
        self._provider = api_provider
        self._client_params: dict = client_params
        self._model_params: dict = model_params
        self._price: ApiPrice = price

        # 数据容器
        self._context: Context = context
        self._image_cache: dict[str, str] = {}  # 用于储存照片的完整base64缓存
        self._queue = asyncio.Queue()
        self._ask_tasks: dict[str, AskTask] = {}
        self._tools_mapping: dict[str, Callable | Awaitable] = {}
        self._last_usage: TokenUsage = TokenUsage()
        self._usage_record: list[LLMUsage] = usage_record
        self._context_use: int = 0

        # 状态位和工具
        self._id = str(time.time_ns())
        self._keep_alive = keep_alive
        self._running = asyncio.Event()
        self._running.set()
        self._running_task_id = ""
        self._client: httpx.AsyncClient | None = None
        self._logger = logger
        self._worker_loop = asyncio.create_task(self.run())
        self._first_SSE_retry = True
        if self._keep_alive:
            self._update_client()

    def setting(
            self,
            new_config: ChatConfig
    ):
        if not isinstance(new_config, ChatConfig):
            return

        if new_config.endpoint is not None:
            self._endpoint = new_config.endpoint

        if new_config.token is not None:
            self._token = new_config.token

        if new_config.model is not None:
            if self._context._usage_accumulation:
                self._context._usage_accumulation.clear()

            self._model = new_config.model

        if new_config.system_prompt is not None:
            self._context.append(SystemPrompt(new_config.system_prompt))

        if new_config.api_provider is not None:
            self._provider = new_config.api_provider

        if new_config.price is not None:
            if isinstance(new_config.price, ApiPrice):
                self._price = new_config.price

        if new_config.max_context is not None:
            if isinstance(new_config.max_context, int):
                self._max_context = new_config.max_context

        if new_config.auto_compress_rate is not None:
            if isinstance(new_config.auto_compress_rate, (float, int)):
                self._compress_rate = new_config.auto_compress_rate

        if new_config.client_params is not None:
            self._client_params = new_config.client_params

        if new_config.model_params is not None:
            self._model_params = new_config.model_params

        if new_config.keep_alive is not None:
            self._keep_alive = new_config.keep_alive

        if new_config.logger is not None:
            self._logger = new_config.logger

        self._logger.info(f"已更新配置。当前模型: [{self._model}]")

    def _update_client(self):
        proxies = self._client_params.pop("proxies", {})
        proxy_mounts = parse_proxies_to_httpx(proxies)
        headers = self._header_constructor()
        self._client = httpx.AsyncClient(mounts=proxy_mounts, headers=headers, **self._client_params)

    def _load_image_from_cache(self, hash_list: list[str]) -> bool:
        for hash_name in hash_list:
            try:
                file_path = get_file_path(hash_name)

            except FileNotFoundError:
                return False

            else:
                with file_path.open('rb') as f:
                    load_content = f.read()

                base64_str = base64.b64encode(load_content).decode()
                self._image_cache[hash_name] = base64_str

        return True

    def _header_constructor(self):
        headers = {"openai": {}, "anthropic": {}}
        if self._provider.lower() not in headers:
            raise ValueError(f"使用了不支持的API供应商格式: {self._provider}")

        headers["openai"].update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        })
        headers["anthropic"].update({
            "x-api-key": self._token,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
            "Connection": "keep-alive"
        })
        return headers[self._provider.lower()]

    async def _payload_constructor(self, context: Context) -> dict:
        provider = self._provider.lower()
        system_prompt = ""
        messages = []
        tools = []
        user_content = {
            "role": "user",
            "content": []
        }
        assistant_content: dict[str, str | list | None] = {"role": "assistant"}

        def _commit_user():
            if user_content["content"]:
                messages.append(user_content.copy())
                user_content.clear()
                user_content.update({
                    "role": "user",
                    "content": []
                })

        def _commit_assistant():
            if "content" in assistant_content or "tool_calls" in assistant_content:
                if "content" not in assistant_content:
                    assistant_content["content"] = None

                messages.append(assistant_content.copy())
                assistant_content.clear()
                assistant_content.update({"role": "assistant"})

        for prompt in context:
            if isinstance(prompt, SystemPrompt):
                if provider == "anthropic":
                    system_prompt = prompt.text

                elif provider == "openai":
                    new_message = {
                        "role": prompt.role,
                        "content": prompt.text
                    }
                    if messages:
                        if messages[0]["role"] == prompt.role:
                            messages[0] = new_message

                        else:
                            messages.insert(0, new_message)

                    else:
                        messages.append(new_message)

                else:
                    raise ValueError(f"使用了不支持的API供应商格式: {provider}")

            elif isinstance(prompt, TextPrompt):
                if prompt.role == "user":
                    _commit_assistant()
                    user_content["content"].append({
                        "type": "text",
                        "text": prompt.text
                    })

                else:
                    _commit_user()
                    if provider == "openai":
                        assistant_content["content"] = prompt.text

                        # 新增：thinking 模型需要把上一轮 reasoning_content 原样回传
                        if prompt.reasoning_content:
                            assistant_content["reasoning_content"] = prompt.reasoning_content

                    elif provider == "anthropic":
                        if "content" not in assistant_content:
                            assistant_content["content"] = []

                        assistant_content["content"].append({
                            "type": "text",
                            "text": prompt.text
                        })

            elif isinstance(prompt, ImagePrompt):
                if prompt.image:
                    if any(hash_name not in self._image_cache for hash_name in prompt.image):
                        success = self._load_image_from_cache(prompt.image)
                        if not success:
                            prompt.image.clear()

                if not prompt.image:
                    await prompt.load_image(self._image_cache)

                if not prompt.image:
                    continue

                if prompt.role == "user":
                    _commit_assistant()
                    if provider == "openai":
                        for hash_name in prompt.image:
                            user_content["content"].append({
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{self._image_cache[hash_name]}"
                                }
                            })

                    elif provider == "anthropic":
                        for hash_name in prompt.image:
                            user_content["content"].append({
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/jpeg",
                                    "data": self._image_cache[hash_name]
                                }
                            })

                    else:
                        raise ValueError(f"使用了不支持的API供应商格式: {provider}")

                else:
                    raise RuntimeError(f"尚未开发生图接口")

            elif isinstance(prompt, ToolCall):
                _commit_user()
                if provider == "openai":
                    if "tool_calls" not in assistant_content:
                        assistant_content["tool_calls"] = []

                    assistant_content["tool_calls"].append({
                        "id": prompt.call_id,
                        "type": "function",
                        "function": {
                            "name": prompt.function_name,
                            "arguments": json.dumps(prompt.arguments, ensure_ascii=False)
                        }
                    })

                elif provider == "anthropic":
                    if "content" not in assistant_content:
                        assistant_content["content"] = []

                    assistant_content["content"].append({
                        "type": "tool_use",
                        "id": prompt.call_id,
                        "name": prompt.function_name,
                        "input": prompt.arguments
                    })

                else:
                    raise ValueError(f"使用了不支持的API供应商格式: {provider}")

            elif isinstance(prompt, ToolResponse):
                _commit_assistant()
                if provider == "openai":
                    messages.append({
                        "role": "tool",
                        "tool_call_id": prompt.call_id,
                        "content": json.dumps(prompt.response, ensure_ascii=False)
                    })

                elif provider == "anthropic":
                    user_content["content"].append({
                        "type": "tool_result",
                        "tool_use_id": prompt.call_id,
                        "content": json.dumps(prompt.response, ensure_ascii=False)
                    })

                else:
                    raise ValueError(f"使用了不支持的API供应商格式: {provider}")

            elif isinstance(prompt, ToolInput):
                tool_schema = build_tool_payloads(prompt.function)
                tool_schema["openai"]["function"]["name"] = prompt.function_name
                tool_schema["anthropic"]["name"] = prompt.function_name
                if provider == "openai":
                    tools.append(tool_schema["openai"])

                elif provider == "anthropic":
                    tools.append(tool_schema["anthropic"])

                else:
                    raise ValueError(f"使用了不支持的API供应商格式: {provider}")

            else:
                raise ValueError(f"Prompts对象错误，无法构建payload")

        _commit_assistant()
        _commit_user()

        payload = {
            **self._model_params,
            "model": self._model,
            "messages": messages
        }

        if system_prompt:
            payload["system"] = system_prompt

        if tools:
            payload["tools"] = tools

        if payload.get("stream", False):
            if provider == "openai":
                payload.setdefault("stream_options", {})  # type: ignore
                payload["stream_options"]["include_usage"] = True  # type: ignore

        # _log.debug(payload)
        # _log.debug(context)
        return payload

    @staticmethod
    def _decode_response(
            response_collect: list,
            reasoning_collect: list,
            tool_calls_collect: dict,
            usage: TokenUsage,
            start_time: float
    ) -> Prompts:
        now = str(time.time())
        result = Prompts(create_time=now, duration=time.time() - start_time)
        text_response = ''.join(response_collect).strip()
        reasoning_response = ''.join(reasoning_collect or []).strip()
        if text_response or reasoning_response:
            final_response = TextPrompt(
                role="assistant",
                text=text_response,
                reasoning_content=reasoning_response,
                create_time=now
            )
            result.append(final_response)

        for call_id in tool_calls_collect:
            call_data = tool_calls_collect[call_id]
            raw_args = call_data.get("arguments", "")
            if not raw_args:
                parsed_args = {}

            else:
                try:
                    parsed_args = json.loads(raw_args)

                except JSONDecodeError:
                    parsed_args = {}

            tc = ToolCall(
                call_data["function_name"],
                call_data["call_id"],
                parsed_args
            )
            result.append(tc)

        if usage:
            result.append(usage)

        # _log.debug(result)
        return result

    def _parse_usage_from_openai(self, usage_dict: dict) -> TokenUsage:
        if not usage_dict:
            return TokenUsage(model=self._model)

        input_t = int(usage_dict.get("prompt_tokens", 0) or 0)
        output_t = int(usage_dict.get("completion_tokens", 0) or 0)
        details = usage_dict.get("prompt_tokens_details") or {}
        cache_hit_t = int(details.get("cached_tokens", 0) or usage_dict.get("cached_tokens", 0) or 0)
        total_t = int(usage_dict.get("total_tokens", input_t + output_t) or 0)
        return TokenUsage(
            input_t,
            output_t,
            cache_hit_t,
            total_t,
            self._model
        )

    def _parse_usage_from_anthropic(self, usage_dict: dict) -> TokenUsage:
        if not usage_dict:
            return TokenUsage(model=self._model)

        input_t = int(usage_dict.get("input_tokens", 0) or 0)
        output_t = int(usage_dict.get("output_tokens", 0) or 0)
        # 命中缓存：当前主流字段是 cache_read_input_tokens
        cache_hit_t = int(
            usage_dict.get("cache_read_input_tokens", 0)
            or usage_dict.get("cache_hit_input_tokens", 0)
            or 0
        )
        total_t = int(usage_dict.get("total_tokens", input_t + output_t) or 0)
        return TokenUsage(
            input_t,
            output_t,
            cache_hit_t,
            total_t,
            self._model
        )

    async def _tiktoken_prompts(self, prompts: Prompts) -> int:
        """
        使用本地tokenizer计算字符串token数
        :param prompts:
        :return:
        """
        if prompts is None:
            return 0

        estimate_token = 0
        estimate_str_list = []
        for prompt in prompts:
            if isinstance(prompt, (SystemPrompt, TextPrompt)):
                estimate_str_list.append(prompt.text)

            elif isinstance(prompt, ImagePrompt):
                if not prompt.image:
                    await prompt.load_image(self._image_cache)

                estimate_token += len(prompt.image) * 1440  # 经验预估值

            elif isinstance(prompt, ToolCall):
                estimate_str_list.append(prompt.function_name)
                estimate_str_list.append(json.dumps(prompt.arguments, ensure_ascii=False))

            elif isinstance(prompt, ToolResponse):
                estimate_str_list.append(prompt.function_name)
                estimate_str_list.append(json.dumps(prompt.response, ensure_ascii=False))

            elif isinstance(prompt, ToolInput):
                tool_schema = build_tool_payloads(prompt.function)
                tool_schema["openai"]["function"]["name"] = prompt.function_name
                tool_schema["anthropic"]["name"] = prompt.function_name
                estimate_str_list.append(json.dumps(tool_schema[self._provider.lower()], ensure_ascii=False))

        full_payload = '\n'.join(estimate_str_list)
        try:
            enc = tiktoken.get_encoding("o200k_base")
            return len(enc.encode(full_payload)) + estimate_token

        except Exception:
            return max(0, int(len(full_payload) / 3)) + estimate_token

    async def _estimate_context_windows(self, new_prompts: Prompts) -> int:
        cal_context = self._context.copy()
        cal_context.append(new_prompts)
        return await self._tiktoken_prompts(cal_context.to_prompts())

    async def run(self):
        while True:
            task: AskTask = await self._queue.get()
            # self._logger.debug("已获取任务")
            await self._running.wait()
            await asyncio.sleep(0.5)  # 等待0.5秒，让no_wait任务先行
            await self._running.wait()
            self._running.clear()

            try:
                run_handle = asyncio.create_task(self.post(task))
                task.handle = run_handle
                response = await asyncio.wait_for(run_handle, timeout=task.timeout)
                task.response = response

                if response is not None:
                    self._context.append(task.prompts)
                    self._context.append(response)

            except asyncio.TimeoutError:
                self._logger.error("LLM请求已超时")
                continue

            except asyncio.CancelledError:
                self._logger.error("LLM请求已取消")
                continue

            except Exception as E:
                self._logger.error(E)
                self._logger.debug(traceback.format_exc())
                continue

            finally:
                if not self._keep_alive and isinstance(self._client, httpx.AsyncClient):
                    asyncio.create_task(self._client.aclose())
                    self._client = None

                self._logger.debug("已完成一轮")
                self._running.set()
                task.event.set()
                self._queue.task_done()

    async def post(self, task: AskTask) -> Prompts | None:
        self._running_task_id = task.id
        self._logger.debug("正在开始请求...")
        if not self._endpoint:
            raise ValueError("尚未设置API链接")

        if task.event.is_set():
            return None

        start_time = time.time()
        new_prompt = task.prompts
        _log.debug(new_prompt)
        context = self._context.copy()
        context.append(new_prompt)
        payload = await self._payload_constructor(context)
        timeout = task.timeout
        provider = self._provider.lower()

        if self._client is None:
            self._update_client()

        response_collect: list[str] = []
        reasoning_collect: list[str] = []
        tool_calls_collect = {}
        round_usage = TokenUsage(model=self._model)

        async def _update_usage(_output: Prompts):
            if round_usage:
                self._context_use = round_usage.input_t + round_usage.output_t
                _GLOBAL_USAGE.update(
                    model=self._model,
                    update_data={
                        "input": round_usage.input_t,
                        "output": round_usage.output_t,
                        "cached": round_usage.cache_hit_t
                    },
                    price=self._price
                )
                for r in self._usage_record:
                    r.update(
                        model=self._model,
                        update_data={
                            "input": round_usage.input_t,
                            "output": round_usage.output_t,
                            "cached": round_usage.cache_hit_t
                        },
                        price=self._price
                    )

            else:
                cached = await self._tiktoken_prompts(self._context.to_prompts())
                input_t = await self._tiktoken_prompts(context.to_prompts())
                output = await self._tiktoken_prompts(_output)
                self._context_use = input_t + output
                _GLOBAL_USAGE.update(
                    model=self._model,
                    update_data={
                        "input": input_t,
                        "output": output,
                        "cached": cached
                    },
                    price=self._price,
                    verified=False
                )
                for r in self._usage_record:
                    r.update(
                        model=self._model,
                        update_data={
                            "input": input_t,
                            "output": output,
                            "cached": cached
                        },
                        price=self._price,
                        verified=False
                    )

        def _display_usage():
            self._last_usage = round_usage
            self._logger.info(
                f"本轮消耗 | 总tokens: [{round_usage.total_t:,}]; "
                f"输入: [{round_usage.input_t:,}]; "
                f"输出: [{round_usage.output_t:,}]; "
                f"输入(缓存): [{round_usage.cache_hit_t:,}]; "
                f"缓存命中率: [{round_usage.cache_hit_t / round_usage.input_t if round_usage.input_t else 0:.2%}]")

        def _parse_json(_data: dict):
            nonlocal round_usage
            if provider == "openai":
                _usage = _data.get("usage", {})
                _new_usage = self._parse_usage_from_openai(_usage)
                round_usage = max(round_usage, _new_usage)
                for _resp in _data["choices"]:
                    _message = _resp["message"]

                    # 思维链
                    if "reasoning_content" in _message and _message["reasoning_content"] is not None:
                        reasoning_collect.append(_message["reasoning_content"])

                    # 兼容字段
                    if "reasoning" in _message and _message["reasoning"] is not None:
                        reasoning_collect.append(str(_message["reasoning"]))

                    # 普通文本
                    if "content" in _message and _message["content"] is not None:
                        response_collect.append(_message["content"])
                        if _message["content"]:
                            self._logger.info(f"LLM: {_message['content']}")

                    if "tool_calls" in _message:
                        for _call in _message["tool_calls"]:
                            _call_id = _call["id"]
                            tool_calls_collect[_call_id] = {
                                "call_id": _call_id,
                                "function_name": _call["function"]["name"],
                                "arguments": _call["function"]["arguments"]
                            }

                    # 兼容旧版function_call
                    if "function_call" in _message and _message["function_call"]:
                        _fc = _message["function_call"]
                        _call_id = _fc.get("id") or "legacy_function_call"
                        tool_calls_collect[_call_id] = {
                            "call_id": _call_id,
                            "function_name": _fc.get("name", ""),
                            "arguments": _fc.get("arguments", "{}")
                        }

            elif provider == "anthropic":
                _usage = _data.get("usage", {})
                _new_usage = self._parse_usage_from_anthropic(_usage)
                round_usage = max(round_usage, _new_usage)
                _blocks = _data["content"]
                for _block in _blocks:
                    if _block["type"] == "tool_use":
                        _call_id = _block["id"]
                        tool_calls_collect[_call_id] = {
                            "call_id": _call_id,
                            "function_name": _block["name"],
                            "arguments": json.dumps(_block["input"], ensure_ascii=False)
                        }

                    elif _block["type"] == "text":
                        response_collect.append(_block["text"])
                        if _block["text"]:
                            self._logger.info(f"LLM: {_block['text']}")

            else:
                raise ValueError(f"使用了不支持的API供应商格式: {provider}")

        try:
            if self._model_params.setdefault("stream", True):
                async with self._client.stream(
                    "POST",
                    url=self._endpoint,
                    json=payload,
                    timeout=timeout
                ) as response:
                    if response.status_code >= 300:
                        self._first_SSE_retry = False
                        try:
                            error_body = await response.aread()
                            error_data = json.loads(error_body) if error_body else {}

                        except:
                            error_data = {}

                        error_msg = error_data.get('error', {}).get('message', {}) or error_data.get("message", 'Unknown error')
                        raise httpx.HTTPError(f"status code: {response.status_code}, message: {error_msg}")

                    response.raise_for_status()  # 保底
                    reasoning_tips = False
                    try_handle_first_line = True
                    with self._logger.info("LLM：", stream=True) as h:
                        if provider == "openai":
                            async for line in response.aiter_lines():
                                if task.event.is_set():
                                    if round_usage:
                                        _display_usage()
                                        return Prompts(round_usage)

                                    return None

                                if not line.startswith("data: "):
                                    if self._first_SSE_retry and try_handle_first_line:
                                        try:
                                            data = json.loads(line)

                                        except JSONDecodeError:
                                            pass

                                        else:
                                            if isinstance(data, dict) and "choices" in data:
                                                try:
                                                    _parse_json(data)

                                                except Exception as E:
                                                    self._logger.debug(f"试图解析首行失败: {E}\n\n首行内容: {data}")
                                                    reasoning_collect.clear()
                                                    response_collect.clear()
                                                    tool_calls_collect.clear()

                                                else:
                                                    break

                                    try_handle_first_line = False
                                    continue

                                try_handle_first_line = False
                                line = line[6:]  # 去掉 "data: " 前缀
                                if line == "[DONE]":
                                    break

                                try:
                                    chunk = json.loads(line)

                                except json.JSONDecodeError:
                                    continue

                                chunk_usage = chunk.get("usage", {})
                                if chunk_usage:
                                    new_usage = self._parse_usage_from_openai(chunk_usage)
                                    round_usage = max(round_usage, new_usage)

                                choices = chunk.get("choices", [])
                                if not choices:
                                    continue

                                delta = choices[0].get("delta", {})

                                # 思维链增量
                                if "reasoning_content" in delta and delta["reasoning_content"] is not None:
                                    new_reasoning = delta["reasoning_content"]
                                    reasoning_collect.append(new_reasoning)
                                    if not reasoning_tips:
                                        h.write(new_reasoning)

                                # 兼容部分平台字段命名：reasoning
                                if "reasoning" in delta and delta["reasoning"] is not None:
                                    new_reasoning = delta["reasoning"]
                                    reasoning_collect.append(new_reasoning)
                                    if not reasoning_tips:
                                        h.write(new_reasoning)

                                # 普通文本增量
                                if "content" in delta and delta["content"] is not None:
                                    new_content = delta["content"]
                                    response_collect.append(new_content)
                                    if not reasoning_tips:
                                        h.write("\n\n思维链结束，最终回答: \n\n")
                                        reasoning_tips = True

                                    h.write(new_content)

                                # 处理tool_calls
                                if "tool_calls" in delta:
                                    for tc in delta["tool_calls"]:
                                        index = tc["index"]
                                        new_id = tc.get("id")
                                        if index not in tool_calls_collect:
                                            tool_calls_collect[index] = {
                                                "call_id": tc.get("id", ""),
                                                "function_name": "",
                                                "arguments": ""
                                            }

                                        # 更新名称
                                        if "function" in tc and "name" in tc["function"]:
                                            tool_calls_collect[index]["function_name"] += tc["function"]["name"]

                                        # 更新参数
                                        if "function" in tc and "arguments" in tc["function"]:
                                            tool_calls_collect[index]["arguments"] += tc["function"]["arguments"]

                                        # 更新id
                                        if new_id and not tool_calls_collect[index].get("call_id"):
                                            tool_calls_collect[index]["call_id"] = new_id

                                # 处理旧版tool_calls
                                if "function_call" in delta and delta["function_call"]:
                                    fc = delta["function_call"]
                                    legacy_key = "legacy_function_call"
                                    if legacy_key not in tool_calls_collect:
                                        tool_calls_collect[legacy_key] = {
                                            "call_id": fc.get("id") or legacy_key,
                                            "function_name": "",
                                            "arguments": ""
                                        }

                                    name_part = fc.get("name")
                                    if isinstance(name_part, str):
                                        tool_calls_collect[legacy_key]["function_name"] += name_part

                                    args_part = fc.get("arguments")
                                    if isinstance(args_part, str):
                                        tool_calls_collect[legacy_key]["arguments"] += args_part

                        elif provider == "anthropic":
                            lines_iter = response.aiter_lines()
                            async for line in lines_iter:
                                if task.event.is_set():
                                    if round_usage:
                                        _display_usage()
                                        return Prompts(round_usage)

                                    return None

                                if line.startswith("event: "):
                                    event = line[7:]  # 去掉 "event: "
                                    # 读取紧随其后的数据行
                                    try:
                                        data_line = await lines_iter.__anext__()

                                    except StopAsyncIteration:
                                        break

                                    if not data_line.startswith("data: "):
                                        continue

                                    data_str = data_line[6:]
                                    try:
                                        event_data = json.loads(data_str)

                                    except json.JSONDecodeError:
                                        continue

                                    # 根据事件类型处理
                                    if event == "content_block_start":
                                        index = event_data.get("index")
                                        block = event_data.get("content_block", {})
                                        if block.get("type") == "tool_use":
                                            tool_calls_collect[index] = {
                                                "call_id": block.get("id"),
                                                "function_name": block.get("name"),
                                                "arguments": ""  # 将逐步累积 JSON 字符串
                                            }

                                    elif event == "content_block_delta":
                                        index = event_data.get("index")
                                        delta = event_data.get("delta", {})
                                        if delta.get("type") == "input_json_delta" and index in tool_calls_collect:
                                            # 累加 tool_use 的输入 JSON 片段
                                            tool_calls_collect[index]["arguments"] += delta.get("partial_json", "")

                                        elif delta.get("type") == "thinking_delta":
                                            new_reasoning = delta.get("thinking", "")
                                            reasoning_collect.append(new_reasoning)
                                            if not reasoning_tips:
                                                h.write(new_reasoning)

                                        elif delta.get("type") == "text_delta":
                                            # 普通文本增量
                                            new_content = delta.get("text", "")
                                            if new_content and not reasoning_tips:
                                                h.write("\n\n思维链结束，最终回答: \n\n")
                                                reasoning_tips = True

                                            response_collect.append(new_content)
                                            h.write(new_content)

                                    # 处理usage
                                    elif event == "message_start":
                                        usage_obj = event_data.get("message", {}).get("usage", {})
                                        new_usage = self._parse_usage_from_anthropic(usage_obj)
                                        round_usage = max(round_usage, new_usage)

                                    elif event == "message_delta":
                                        usage_obj = event_data.get("usage", {})
                                        new_usage = self._parse_usage_from_anthropic(usage_obj)
                                        round_usage = max(round_usage, new_usage)

                                    # 处理其他无效事件
                                    elif event == "error":
                                        # Anthropic SSE 可能在 HTTP 200 后发错误事件
                                        err = event_data.get("error", {}) if isinstance(event_data, dict) else {}
                                        msg = err.get("message", "unknown anthropic sse error")
                                        raise httpx.HTTPError(f"Anthropic SSE error: {msg}")

                                    elif event == "ping":
                                        # 心跳事件，忽略即可
                                        pass

                                    elif event == "content_block_stop":
                                        # 先忽略，不中断；后续需要可扩展 token usage/stop_reason
                                        pass

                                    elif event == "message_stop":
                                        # 流结束
                                        break

                        else:
                            raise ValueError(f"使用了不支持的API供应商格式: {provider}")

            else:
                response = await self._client.post(
                    url=self._endpoint,
                    json=payload,
                    timeout=timeout
                )
                if response.status_code >= 300:
                    self._first_SSE_retry = False
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error', {}).get('message', {}) or error_data.get("message", 'Unknown error')
                    raise httpx.HTTPError(f"status code: {response.status_code}, message: {error_msg}")

                response.raise_for_status()  # 保底
                data: dict = response.json()
                _parse_json(data)

        except asyncio.CancelledError:
            self._first_SSE_retry = False
            self._logger.debug("请求任务已取消")
            if round_usage:
                _display_usage()
                return Prompts(round_usage)

        except Exception as E:
            self._first_SSE_retry = False
            _log.error(E)
            _log.debug(traceback.format_exc())
            if round_usage:
                _display_usage()
                return Prompts(round_usage)

        result = None
        if response_collect or reasoning_collect or tool_calls_collect:
            final_text = ''.join(response_collect).strip()
            if not final_text:
                self._logger.info(f"无文本内容")

            # else:
                # self._logger.debug(final_text)

            result = self._decode_response(
                response_collect,
                reasoning_collect,
                tool_calls_collect,
                round_usage,
                start_time
            )

        else:
            self._logger.info(f"无返回内容")

        if round_usage:
            _display_usage()

        await _update_usage(result)
        return result

    async def post_nowait(self, task: AskTask) -> Prompts | None:
        await self._running.wait()
        self._running.clear()
        try:
            run_handle = asyncio.create_task(self.post(task))
            task.handle = run_handle
            response = await asyncio.wait_for(run_handle, timeout=task.timeout)

        except asyncio.TimeoutError:
            self._logger.error("LLM请求已超时")
            return None

        except asyncio.CancelledError:
            self._logger.error("LLM请求已取消")
            return None

        except Exception as E:
            self._logger.error(E)
            self._logger.debug(traceback.format_exc())
            return None

        else:
            if response is not None:
                self._context.append(task.prompts)
                self._context.append(response)

            return response

        finally:
            if not self._keep_alive and self._client is not None:
                await self._client.aclose()
                self._client = None

            self._running.set()

    # 主要接口
    async def ask(
            self,
            new_ask: Prompts | BasePrompt | list | str,
            *,
            task_id: str = None,
            queue: bool = True,
            no_wait: bool = False,
            timeout: float = None
    ) -> Prompts | None:
        """
        使用new_ask开始新一轮对话请求，并返回响应
        :param new_ask: Prompts对象
        :param task_id: 指定任务ID
        :param queue: 是否进入任务队列等待其他任务。若为False，则有任务在运行时直接跳过，不会保存Prompts
        :param no_wait: 是否不进入任务队列直接开始，若为True，则在这一个任务结束后直接运行
        :param timeout: 超时/秒
        :return: 响应的Prompts对象
        """
        if timeout is None or timeout <= 0:
            timeout = SETTING_CFG.LLM.ChatRequestTimeout  # 默认300

        if not new_ask:
            return None

        start_time = time.time()
        if not isinstance(new_ask, Prompts):
            if isinstance(new_ask, BasePrompt):
                new_ask = Prompts(new_ask)

            elif isinstance(new_ask, list):
                new_ask = Prompts(new_ask)

            elif isinstance(new_ask, str):
                new_ask = Prompts(TextPrompt(role="user", text=new_ask))

            else:
                raise TypeError

        # 预估检查超窗
        if self._compress_rate is None:
            compress_rate = 0.7

        else:
            compress_rate = max(min(self._compress_rate, 0.9), 0.3)

        if self._max_context is not None and self._max_context > 0:
            if self._last_usage.input_t >= (compress_rate - 0.1) * self._max_context:
                _log.debug(f"当前context: {self._last_usage.input_t:,}")
                estimate_tokens = await self._estimate_context_windows(new_ask)
                _log.debug(f"预估context: {estimate_tokens:,}")
                if estimate_tokens >= compress_rate * self._max_context:
                    raise ContextOverflowError

        if task_id is not None:
            new_task = AskTask(new_ask, timeout, task_id)

        else:
            new_task = AskTask(new_ask, timeout)

        while new_task.id in self._ask_tasks:
            new_id = int(new_task.id)
            new_id += 1
            new_task.id = str(new_id)

        self._ask_tasks[new_task.id] = new_task
        if no_wait:
            response = await self.post_nowait(new_task)
            del self._ask_tasks[new_task.id]
            self._logger.info(f"耗时：[{int(time.time() - start_time)}]秒")

            # 首次失败自动重试（条件）
            if response is None and self._first_SSE_retry:
                self._logger.debug("首次请求解析失败，正在自动重试...")
                self._first_SSE_retry = False
                return await self.ask(new_ask, task_id=task_id, queue=queue, no_wait=no_wait, timeout=timeout)

            self._first_SSE_retry = False
            return response

        if queue:
            await self._queue.put(new_task)
            # self._logger.debug("已提交任务")
            try:
                await asyncio.wait_for(new_task.event.wait(), timeout=timeout)

            except asyncio.TimeoutError:
                self._logger.error("LLM请求已超时")
                return None

            except Exception as E:
                self._logger.error(E)
                self._logger.debug(traceback.format_exc())
                return None

            else:
                self._logger.info(f"耗时：[{int(time.time() - start_time)}]秒")

                # 首次失败自动重试（条件）
                if new_task.response is None and self._first_SSE_retry:
                    self._logger.debug("首次请求解析失败，正在自动重试...")
                    self._first_SSE_retry = False
                    return await self.ask(new_ask, task_id=task_id, queue=queue, no_wait=no_wait, timeout=timeout)

                self._first_SSE_retry = False
                return new_task.response

            finally:
                new_task.event.set()
                if new_task.handle is not None:
                    new_task.handle.cancel()

                del self._ask_tasks[new_task.id]

        else:
            if self._running.is_set():
                response = await self.post_nowait(new_task)
                del self._ask_tasks[new_task.id]
                self._logger.info(f"耗时：[{int(time.time() - start_time)}]秒")

                # 首次失败自动重试（条件）
                if response is None and self._first_SSE_retry:
                    self._logger.debug("首次请求解析失败，正在自动重试...")
                    self._first_SSE_retry = False
                    return await self.ask(new_ask, task_id=task_id, queue=queue, no_wait=no_wait, timeout=timeout)

                self._first_SSE_retry = False
                return response

            else:
                del self._ask_tasks[new_task.id]
                return None

    def add_tools(self, tools: Prompts | list[ToolInput] | ToolInput) -> Prompts:
        self._context.append(tools)
        return self._context.tools

    def add_context(self, new_context: Context | Prompts | BasePrompt) -> Context:
        self._context.append(new_context)
        return self._context.copy()

    @property
    def model(self) -> dict:
        """返回模型信息"""
        return {
            "model": self._model,
            "endpoint": self._endpoint,
            "token": self._token,
            "provider": self._provider,
            "parameter": self._model_params
        }

    @property
    def tools(self) -> Prompts:
        """返回当前工具集"""
        return self._context.tools

    @property
    def context(self) -> Context:
        """返回Context对象的shadow copy"""
        return self._context.copy()

    @property
    def context_use(self) -> float:
        if self._max_context is None or self._max_context <= 0:
            return -1.0

        return self._context_use / self._max_context

    def replace_context(self, new_context: Context):
        """使用外部的Context，让Chat自动管理"""
        self._context = new_context

    async def get_context(self) -> Context:
        """注意，此函数返回的是Context对象本身，不是shadow"""
        return self._context

    def copy(self, new_context=True):
        return Chat(
            endpoint=self._endpoint,
            token=self._token,
            model=self._model,
            max_context=self._max_context,
            system_prompt=self._context.system.text if self._context.system else None,
            api_provider=self._provider,
            price=self._price,
            client_params=copy.deepcopy(self._client_params),
            model_params=self._model_params,
            context=None if new_context else self._context,
            keep_alive=self._keep_alive,
            logger=self._logger,
            usage_record=self._usage_record.copy()
        )

    async def clear(self, system: bool = False, tools: bool = False):
        await self._running.wait()
        self._context.clear(system, tools)

    async def stop_from_id(self, task_id: str):
        if task_id not in self._ask_tasks:
            return

        task = self._ask_tasks[task_id]
        if task.event.is_set():
            return

        task.event.set()
        await asyncio.sleep(0)  # 抛出控制权试图让请求自然停止
        if task.handle is not None:
            task.handle.cancel()

    async def stop_one(self):
        if not self._running_task_id or self._running_task_id not in self._ask_tasks:
            return

        await self.stop_from_id(self._running_task_id)

    async def stop_all(self):
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()

            except asyncio.QueueEmpty:
                break

        for task_id in self._ask_tasks:
            await self.stop_from_id(task_id)

    async def close(self):
        if self._client is not None:
            try:
                await asyncio.wait_for(self._client.aclose(), timeout=3)

            except asyncio.TimeoutError:
                pass

            self._client = None

        self._worker_loop.cancel()

    def __del__(self):
        self._image_cache.clear()


def parse_llm_setting(preset_name: str) -> ChatConfig | None:
    new_setting = ChatConfig()
    llm = get_llm(preset_name)
    if not (llm.model and llm.endpoint):
        raise ValueError(f"LLM预设[{preset_name}]未设置endpoint和模型")

    if llm.proxy_mode:
        proxy = get_proxy(llm.proxy_mode)
        new_setting.client_params = {"proxies": proxy}

    if llm.price:
        if not isinstance(llm.price, dict):
            raise TypeError(
                f"LLM预设[{preset_name}]价格参数类型错误:\n"
                f"Key <price> should be <dict>, got {type(llm.price)}")

        mirror = llm.price.copy()
        if mirror:
            new_price = ApiPrice(
                currency=mirror.get("currency", "CNY"),
                input=float(mirror.get("input_token", 0.0)),
                output=float(mirror.get("output_token", 0.0)),
                cached=float(mirror.get("cache_hit", 0.0))
            )
            new_setting.price = new_price

    if llm.extra_parameter:
        if not isinstance(llm.extra_parameter, dict):
            raise TypeError(
                f"LLM预设[{preset_name}]额外参数类型错误:\n"
                f"Key <extra_parameter> should be <dict>, got {type(llm.extra_parameter)}")

        new_setting.model_params = llm.extra_parameter.copy()

    for key in ("endpoint", "token", "model", "api_type"):
        if not isinstance(llm[key], str):
            raise TypeError(f"LLM预设[{preset_name}]基本参数类型错误:\n"
                            f"Key <{key}> should be <str>, got {type(llm[key])}")

    auto_compress_gate = llm.get("auto_compress_gate", llm.get("auto_compress_rate"))
    if not isinstance(llm["max_context"], (int, float)):
        raise TypeError(
            f"LLM预设[{preset_name}]context相关参数类型错误:\n"
            f"Key <max_context> should be <int | float>, got {type(llm['max_context'])}")
    if not isinstance(auto_compress_gate, (int, float)):
        raise TypeError(
            f"LLM预设[{preset_name}]context相关参数类型错误:\n"
            f"Key <auto_compress_gate> should be <int | float>, got {type(auto_compress_gate)}")

    new_setting.endpoint = llm.endpoint
    new_setting.token = llm.token
    new_setting.model = llm.model
    new_setting.api_provider = llm.api_type
    new_setting.max_context = llm.max_context
    new_setting.auto_compress_rate = float(auto_compress_gate)

    return new_setting
