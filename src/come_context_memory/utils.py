from __future__ import annotations

import time
import copy
import os
import math
import json
import shutil
import asyncio
import re
import threading
from typing import Any, Callable, Awaitable, Literal, Generic, TypeVar, Optional
from datetime import datetime
from io import BytesIO
from pathlib import Path


_T = TypeVar('_T')  # 通用类型
_K = TypeVar('_K')  # 键类型
_V = TypeVar('_V')  # 值类型


class _EmptyType:
    """占位符"""
    def __init__(self):
        ...


class DecoratedDict(dict):
    """
    自定义字典类，可以直接通过属性修改字典数据
    """
    PROTECTED_METHODS = {
        'clear', 'copy', 'fromkeys', 'get', 'items', 'keys',
        'pop', 'popitem', 'setdefault', 'update', 'values'
    }

    def __init__(self, *args, **kwargs):
        def transform_list(_list: list):
            for _ in range(len(_list)):
                if isinstance(_list[_], dict):
                    # if not _list[_]:
                    #     continue

                    _list[_] = DecoratedDict(_list[_])

                elif isinstance(_list[_], list):
                    # if not _list[_]:
                    #     continue

                    transform_list(_list[_])

        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict):
                # if not v:
                #    continue

                self[k] = DecoratedDict(v)

            elif isinstance(v, list):
                # if not v:
                #     continue

                transform_list(v)

    def __setattr__(self, key, value):

        # 处理property
        cls_attr = getattr(type(self), key, None)
        if isinstance(cls_attr, property):
            if cls_attr.fset:
                return cls_attr.fset(self, value)

            else:
                raise AttributeError(f"can't set attribute '{key}'")

        if key in self.PROTECTED_METHODS:
            raise AttributeError(f"'{key}' is a builtin method that cannot use to dict key")

        if isinstance(value, dict):
            value = DecoratedDict(value)

        self[key] = value

    def __getattr__(self, item):
        # 首先检查是否是property
        cls_attr = getattr(type(self), item, None)
        if isinstance(cls_attr, property):
            if cls_attr.fget is None:
                raise AttributeError(f"unreadable attribute '{item}'")

            return cls_attr.fget(self)  # 调用property的getter

        # 如果是受保护的方法，返回原始方法
        if item in self.PROTECTED_METHODS:
            return getattr(super(), item)

        try:
            if item not in self:
                self[item] = DecoratedDict()
                return self[item]

            else:
                sub = self.get(item)
                if type(sub) is dict:
                    self[item] = DecoratedDict(sub)

                return self[item]

        except KeyError:
            raise AttributeError(f"'{__class__.__name__}' object has no attribute '{item}'")

    def __deepcopy__(self, memo=None):
        if memo is None:
            memo = {}

        if id(self) in memo:
            return memo[id(self)]

        # 创建一个新的DecoratedDict实例
        new_dict = DecoratedDict()
        memo[id(self)] = new_dict

        # 将当前字典的所有项复制到新字典中
        for key, value in self.items():
            # 使用copy.deepcopy递归复制每个值
            new_dict[key] = copy.deepcopy(value, memo)

        return new_dict


class CommandSet:
    def __init__(self):
        self.handler_open = {}
        self.handler_admin = {}

    def add_handler_open(self, key: str):
        """
        使用装饰器添加普通命令
        :param key:
        :return:
        """

        def decorator(func: Awaitable | Callable):
            self.handler_open[key] = {"func": func, "doc": func.__doc__}
            return func

        return decorator

    def add_handler_admin(self, key: str):
        """
        使用装饰器添加管理员命令
        :param key:
        :return:
        """

        def decorator(func: Awaitable | Callable):
            self.handler_admin[key] = {"func": func, "doc": func.__doc__}
            return func

        return decorator


class RequestsResponse(DecoratedDict):
    """
    requests下载器的数据类
    done: 下载是否成功。 type: bool
    data: 若成功下载，则有数据。 type: requests.Response
    message: 若下载失败，则保存错误信息。 type: str
    retry: 下载的总次数。 type: int
    duration: 下载总共的耗时，时间戳。 type: float
    """
    from requests import Response

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def done(self) -> bool:
        return self.get("done", False)

    @property
    def data(self) -> Response | None:
        return self.get("data", None)

    @property
    def retry(self) -> int:
        return self.get("retry", 0)

    @property
    def duration(self) -> float:
        return self.get("duration", 0)

    @property
    def message(self) -> str:
        return self.get("message", "")


class HttpxResponse:
    from httpx import Response
    def __init__(self, response: Response):
        self._object = response
        self._content = None
        self._text = ""
        self._message = ""
        self._errors = None
        self._ok: bool = False
        self._response_handle()

    def _response_handle(self):
        # 检查响应是否成功
        if self._object.status_code not in [200, 206]:
            return self._fail_handle()

        self._ok = True
        return self._decompress()

    def _fail_handle(self):
        """处理失败的响应"""
        try:
            fail = json.loads(self._object.text)
            self._message = fail.get("message", "")
            self._errors = fail.get("errors", None)

        except Exception:
            pass

    def _success_handle(self):
        """处理成功的响应"""
        self._content = self._object.content
        self._text = self._object.text

    def _decompress(self):
        """解压数据"""
        content_encoding = self._object.headers.get('content-encoding', '').lower()
        SUPPORTED_ENCODINGS = {
            'gzip': self._gzip,
            'deflate': self._gzip,
            'br': self._brotli,
            'zstd': self._zstd
        }
        if not content_encoding:
            self._success_handle()
            return

        # 可能有多个编码，如 'gzip, zstd'
        encodings = [e.strip() for e in content_encoding.split(',')]

        # 返回第一个支持的编码
        for enc in encodings:
            if enc in SUPPORTED_ENCODINGS:
                SUPPORTED_ENCODINGS[enc]()
                return

        self._success_handle()
        return

    def _zstd(self):
        import zstandard as zstd
        data = self._object.content
        try:
            dctx = zstd.ZstdDecompressor()
            reader = dctx.stream_reader(self._object.content)
            self._content = dctx.decompress(data)
            self._text = reader.read().decode()

        except Exception:
            pass

    def _gzip(self):
        import gzip
        self._text = self._object.text
        data = self._object.content
        try:
            self._content = gzip.decompress(data)

        except Exception:
            try:
                import zlib
                self._content = zlib.decompress(data, -zlib.MAX_WBITS)

            except Exception:
                pass

    def _brotli(self):
        import brotli
        self._text = self._object.text
        data = self._object.content
        try:
            self._content = brotli.decompress(data)

        except Exception:
            pass

    @property
    def object(self) -> Response:
        """httpx库原本的Response对象"""
        return self._object

    @property
    def content(self) -> None | bytes:
        """尝试解压后的二进制数据"""
        return self._content

    @property
    def text(self) -> str:
        """尝试解码后的文本表示"""
        return self._text

    @property
    def message(self) -> str:
        """仅当失败时：失败信息"""
        return self._message

    @property
    def errors(self) -> None | list | dict:
        """仅当失败时：错误详情"""
        return self._errors

    @property
    def ok(self) -> bool:
        """是否成功"""
        return self._ok


class ARLock:
    """异步可重入锁（task-owned）"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: Optional[asyncio.Task] = None
        self._count = 0
        self._waiter = 0  # 近似等待数，不保证严格精确

    @staticmethod
    def _current_task_or_raise() -> asyncio.Task:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("ARLock must be used inside a running asyncio Task")
        return task

    async def acquire(self) -> None:
        current = self._current_task_or_raise()

        # 同一任务重入
        if self._owner is current:
            self._count += 1
            return

        self._waiter += 1
        try:
            await self._lock.acquire()
        finally:
            self._waiter -= 1

        self._owner = current
        self._count = 1

    def release(self) -> None:
        current = self._current_task_or_raise()

        if self._owner is not current:
            raise RuntimeError("Cannot release un-acquired lock")

        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    async def __aenter__(self) -> ARLock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.release()

    @property
    def locked(self) -> bool:
        return self._lock.locked()

    @property
    def owner(self) -> Optional[asyncio.Task]:
        return self._owner

    @property
    def count(self) -> int:
        current = asyncio.current_task()
        return self._count if self._owner is current else 0

    @property
    def waiter(self) -> int:
        return self._waiter


class AutoMapping(Generic[_V]):
    """
    自动管理的映射表
    支持周期持久化、自动清理过期键
    """
    def __init__(
            self,
            save_path: Path = None,
            persist_period: Literal["UPDATE", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK"] = "MINUTE",
            expire_day: int | Callable[..., int] = 0,
            default: dict[str, _V] = None
    ):
        """
        自动管理的映射表
        支持周期持久化、自动清理过期键
        :param save_path: 持久化文件路径
        :param persist_period: 持久化周期，UPDATE为每次读取
        :param expire_day: 大于0时自动清理过期项目
        """
        self._save_path = save_path
        self._period = persist_period
        self._expire_day: int | Callable[..., int] = expire_day
        self._expire = -1
        self._last_persist = time.time()
        self._mapping: dict[str, dict] = {}
        self._lock = threading.Lock()

        if isinstance(default, dict) and default:
            for k, v in default.items():
                self._mapping[k] = {
                    "update": time.time(),
                    "data": v
                }

        if isinstance(self._save_path, Path):
            self._save_path.parent.mkdir(parents=True, exist_ok=True)

        self._initiate()

    def _initiate(self):
        if self._save_path is None:
            return

        if not self._save_path.is_file():
            return

        try:
            load_content = json.loads(self._save_path.read_text(encoding="utf-8"))

        except json.JSONDecodeError:
            return

        if not isinstance(load_content, dict):
            return

        with self._lock:
            self.load_dict(load_content)

    def to_dict(self):
        save = {
            "update": self._last_persist,
            "description": "此文件为映射表持久化数据",
            "version": "0.1.0",
            "data": self._mapping
        }
        return save

    def load_dict(self, data: dict):
        mapping = data.get("data", {})
        last = data.get("update", time.time())
        self._mapping.update(mapping)
        self._last_persist = last

    def _automatic(self):
        now = time.time()
        if self._period == "WEEK":
            if now - self._last_persist < 3600 * 24 * 7:
                return

        elif self._period == "DAY":
            if now - self._last_persist < 3600 * 24:
                return

        elif self._period == "HOUR":
            if now - self._last_persist < 3600:
                return

        elif self._period == "MINUTE":
            if now - self._last_persist < 60:
                return

        elif self._period == "SECOND":
            if now - self._last_persist < 1:
                return

        self._last_persist = now

        # 检查过期
        # 延迟加载配置
        if self._expire == -1:
            expire_day = self._expire_day
            if callable(self._expire_day):
                expire_day = self._expire_day = self._expire_day()

            if expire_day > 0:
                self._expire = expire_day * 24 * 3600

        if self._expire > 0:
            expired = []
            for k, v in self._mapping.items():
                last = v.setdefault("update", time.time())
                if now - last > self._expire:
                    expired.append(k)

            for k in expired:
                del self._mapping[k]

        # 保存文件
        if self._save_path is None:
            return

        atomic_save_json(self.to_dict(), self._save_path)

    def update(self, key: str, value: _V):
        """更新映射"""
        return self.__setitem__(key, value)

    def update_from_dict(self, data: dict[str, _V]):
        with self._lock:
            for k, v in data.items():
                if not isinstance(k, str):
                    raise TypeError(f"AutoMapping only accept str for key, but get <{type(k)}>")

            content = self._mapping.setdefault(k, {})
            content.update(
                {
                    "update": time.time(),
                    "data": v
                }
            )

    def get(self, key: str, default: Any = None) -> _V:
        if key not in self._mapping:
            return default

        return self.__getitem__(key)

    def remove(self, key: str):
        return self.__delitem__(key)

    def pop(self, key: str, default: Any = None) -> _V:
        with self._lock:
            if key not in self._mapping:
                return default

            data = self._mapping[key].get("data", None)
            del self._mapping[key]
            self._automatic()
            return data

    def keys(self):
        return self._mapping.keys()

    def copy(self) -> dict[str, _V]:
        """
        深复制，返回一个字典，破坏引用关系，不再具备功能
        :return:
        """
        return {k: copy.deepcopy(v["data"]) for k, v in self._mapping.items()}

    def values(self):
        remapping = {k: v.get("data", None) for k, v in self._mapping.items()}
        return remapping.values()

    def items(self):
        remapping = {k: v.get("data", None) for k, v in self._mapping.items()}
        return remapping.items()

    def __contains__(self, item):
        return item in self._mapping

    def __getitem__(self, item) -> _V:
        with self._lock:
            self._mapping[item]["update"] = time.time()
            self._automatic()
            try:
                data = self._mapping[item]["data"]

            except KeyError:
                raise KeyError(f"'{item}' not in mapping")

            else:
                return data

    def __setitem__(self, key, value: _V):
        if not isinstance(key, str):
            raise TypeError(f"AutoMapping only accept str for key, but get <{type(key)}>")

        with self._lock:
            content = self._mapping.setdefault(key, {})
            content.update(
                {
                    "update": time.time(),
                    "data": value
                }
            )
            self._automatic()

    def __delitem__(self, key):
        with self._lock:
            del self._mapping[key]
            self._automatic()


def request_downloader(
        url: str,
        retry: int = 1,
        method: str = "get",
        timeout=None,
        proxy = None,
        extra_parameter: dict = None,
        default: Any = None,
        errors: dict = None,
        success: Callable[[Any], bool] = None,
        sleep: float | Callable[[], float] = None
) -> RequestsResponse:
    """
    封装的一个requests库请求，包含get和post
    :param url:
    :param retry: 重试次数
    :param method: get/post
    :param timeout:
    :param proxy: 字典，为空则使用全局代理
    :param extra_parameter 请求的额外参数
    :param default: 失败时返回的默认结果
    :param errors: 对应200外不同网络状态码的字典
    :param success: 判断成功返回(200)的额外条件，建议使用lambda
    :param sleep: 每次请求暂停的时间，用来规避反爬
    :return: 一个字典{"done: True, data: response, retry: int, duration: float, message: Any}
    """
    import requests

    retries = 0
    message: Any = None
    start_time = time.time()
    if errors is None:
        errors = {}

    if extra_parameter is None:
        extra_parameter = {}

    while retries < retry:
        if sleep is not None:
            if callable(sleep):
                time.sleep(sleep())

            else:
                time.sleep(sleep)

        try:
            if method.lower() == "get":
                response = requests.get(url=url, proxies=proxy, timeout=timeout, **extra_parameter)

            else:
                response = requests.post(url=url, proxies=proxy, timeout=timeout, **extra_parameter)

        except (requests.Timeout, requests.RequestException) as E:
            message = E
            retries += 1
            continue

        if response.status_code != 200:
            if response.status_code in errors:
                message = errors[response.status_code]

            retries += 1
            continue

        if success is not None:
            if not success(response):
                message = "response doesn't match condition"
                retries += 1
                continue

        return RequestsResponse(
            {
                "done": True,
                "data": response,
                "retry": retries + 1,
                "duration": time.time() - start_time,
                "message": ""
            }
        )

    return RequestsResponse(
        {
            "done": False,
            "data": default,
            "retry": retries + 1,
            "duration": time.time() - start_time,
            "message": message
        }
    )

def resize_image(
        image: BytesIO | str | Path | bytes,
        max_size: float
) -> tuple[BytesIO | None, tuple[int, int]]:
    """
    将图片压缩到最大大小以内
    :param image: 图片的BytesIO，字节数据
    :param max_size: mb
    :return:
    """
    from PIL import Image, UnidentifiedImageError

    from come_context_memory.config import SETTING_CFG
    size_limit = max_size * 1024 * 1024
    if isinstance(image, (Path, str)):
        if not os.path.isfile(image):
            return None, (0, 0)

        with open(image, 'rb') as f:
            data = BytesIO(f.read())

    elif isinstance(image, bytes):
        data = BytesIO(image)

    elif hasattr(image, "read") and hasattr(image, "seek"):
        current_pi = image.tell()
        image.seek(0)
        data = BytesIO(image.read())
        image.seek(current_pi)

    else:
        return None, (0, 0)

    current_size = len(data.getvalue())
    try:
        with Image.open(data) as img:
            if current_size <= size_limit:
                return data, img.size

            img_type = img.format
            new_quality = 60

            # 先尝试压缩图片质量
            while new_quality > SETTING_CFG.Common.MinImageQuality:  # 默认20
                new_buffer = BytesIO()
                save_args = {'format': img_type, 'quality': new_quality}
                if img_type == "PNG":
                    save_args['compress_level'] = 9 - int(new_quality / 10)

                else:
                    save_args['optimize'] = True

                img.save(new_buffer, **save_args)
                if new_buffer.tell() < size_limit:
                    new_buffer.seek(0)
                    return new_buffer, img.size

                new_quality -= 5

            # 再尝试压缩分辨率
            orig_width, orig_height = img.size
            for attempt in range(SETTING_CFG.Common.CompressImageSizeAttempt):  # 默认5
                new_width = math.floor(orig_width * SETTING_CFG.Common.CompressImageSizeFactor)  # 默认0.7
                new_height = math.floor(orig_height * SETTING_CFG.Common.CompressImageSizeFactor)
                resized_img = img.resize((new_width, new_height))
                orig_width, orig_height = resized_img.size
                new_buffer = BytesIO()
                resized_img.save(new_buffer, format=img_type, quality=SETTING_CFG.Common.MinImageQuality)
                if new_buffer.tell() < max_size:
                    new_buffer.seek(0)
                    return new_buffer, resized_img.size

            return None, img.size

    except (UnidentifiedImageError, OSError, IOError, PermissionError):
        return None, (0, 0)

def resize_image_to_base64(
        image: str | Path | bytes | BytesIO,
        *,
        max_file_size_mb: float = 5.0,
        gif_max_frames: int = 9,
        output_format: str = "JPEG",
        start_quality: int = 90,
        min_quality: int = None,
        quality_step: int = 5,
        resize_attempts: int = None,
        resize_factor: float = None,
) -> list[str]:
    """将输入图片标准化为可上传给 LLM 的 base64 列表。

    规则：
    - 静态图返回长度为 1 的列表
    - 动图（GIF）按采样拆帧后返回多个元素
    - 任何处理失败都直接抛异常，不返回默认值
    """
    import base64
    from PIL import Image
    from come_context_memory.config import SETTING_CFG
    if min_quality is None:
        min_quality: int = SETTING_CFG.Common.MinImageQuality  # 默认20

    if resize_attempts is None:
        resize_attempts: int = SETTING_CFG.Common.CompressImageSizeAttempt  # 默认5

    if resize_factor is None:
        resize_factor: int = SETTING_CFG.Common.CompressImageSizeFactor  # 默认0.7

    _COMMON_OUTPUT_FORMATS = {"JPEG", "PNG", "WEBP"}

    if max_file_size_mb <= 0:
        raise ValueError("max_file_size_mb 必须大于 0")
    if not 2 <= gif_max_frames <= 9:
        raise ValueError("gif_max_frames 必须在 2 到 9 之间")
    if output_format.upper() not in _COMMON_OUTPUT_FORMATS:
        raise ValueError(f"output_format 仅支持: {sorted(_COMMON_OUTPUT_FORMATS)}")
    if not 0 < resize_factor < 1:
        raise ValueError("resize_factor 必须在 0 到 1 之间")
    if resize_attempts < 0:
        raise ValueError("resize_attempts 不能小于 0")
    if quality_step <= 0:
        raise ValueError("quality_step 必须大于 0")
    if min_quality <= 0 or start_quality <= 0:
        raise ValueError("min_quality 与 start_quality 必须大于 0")
    if min_quality > start_quality:
        raise ValueError("min_quality 不能大于 start_quality")

    def _read_input_bytes(_image) -> bytes:
        """读取输入并统一为字节流。"""
        if isinstance(_image, (str, Path)):
            path = Path(_image)
            if not path.is_file():
                raise FileNotFoundError(f"文件不存在: {path}")
            return path.read_bytes()

        if isinstance(_image, bytes):
            return _image

        if hasattr(_image, "read") and hasattr(_image, "seek"):
            stream = _image
            current_pos = stream.tell()
            stream.seek(0)
            data = stream.read()
            stream.seek(current_pos)
            if not isinstance(data, bytes):
                raise TypeError("文件流读取结果必须是 bytes")
            return data

        raise TypeError("image 仅支持 str | Path | bytes | BinaryIO")

    def _sample_frame_indices(_total_frames: int, max_frames: int) -> list[int]:
        """对 GIF 帧进行均匀采样，并保证首帧与尾帧都被包含。"""
        if _total_frames <= 0:
            raise ValueError("GIF 帧数必须大于 0")

        if _total_frames <= max_frames:
            return list(range(_total_frames))

        # 使用等间隔采样，固定包含首尾帧。
        step_count = max_frames - 1
        sampled = [round(i * (_total_frames - 1) / step_count) for i in range(max_frames)]

        # 四舍五入可能导致重复索引，这里去重并补齐数量。
        deduped: list[int] = []
        seen: set[int] = set()
        for idx in sampled:
            if idx not in seen:
                deduped.append(idx)
                seen.add(idx)

        cursor = 0
        while len(deduped) < max_frames:
            if cursor not in seen:
                deduped.append(cursor)
                seen.add(cursor)
            cursor += 1

        deduped.sort()
        deduped[0] = 0
        deduped[-1] = _total_frames - 1
        return deduped

    def _save_image(_image: Image.Image, *, _quality: int) -> bytes:
        """按指定格式和质量将图片编码为字节。"""
        buffer = BytesIO()

        save_args: dict[str, object] = {"format": output_format}
        if output_format in {"JPEG", "WEBP"}:
            save_args.update({"quality": _quality, "optimize": True})

        if output_format == "PNG":
            # PNG 不使用 quality，改为压缩等级。
            compress_level = max(0, min(9, 9 - int(_quality / 11)))
            save_args.update({"compress_level": compress_level, "optimize": True})

        _image.save(buffer, **save_args)
        return buffer.getvalue()

    def _compress_image_to_limit(
            _image: Image.Image,
            *,
            size_limit_bytes: int,
    ) -> bytes:
        """在大小限制内压缩图片，不满足条件时抛出异常。"""
        working = _image.copy()
        if output_format == "JPEG" and working.mode not in ("RGB", "L"):
            working = working.convert("RGB")

        for _ in range(resize_attempts + 1):
            quality = start_quality
            while quality >= min_quality:
                encoded = _save_image(working, _quality=quality)
                if len(encoded) <= size_limit_bytes:
                    return encoded
                quality -= quality_step

            new_width = max(1, int(working.width * resize_factor))
            new_height = max(1, int(working.height * resize_factor))
            if (new_width, new_height) == working.size:
                break

            working = working.resize((new_width, new_height), Image.Resampling.LANCZOS)

        raise ValueError("无法在给定限制内完成图片压缩，请提高 max_file_size_mb 或放宽压缩参数")

    raw = _read_input_bytes(image)
    size_limit = int(max_file_size_mb * 1024 * 1024)

    with Image.open(BytesIO(raw)) as img:
        output_fmt = output_format.upper()

        if bool(getattr(img, "is_animated", False)):
            total_frames = int(getattr(img, "n_frames", 1))
            frame_indices = _sample_frame_indices(total_frames, gif_max_frames)
            encoded_list: list[str] = []

            for frame_index in frame_indices:
                img.seek(frame_index)
                frame = img.convert("RGB")
                compressed = _compress_image_to_limit(
                    frame,
                    size_limit_bytes=size_limit
                )
                encoded_list.append(base64.b64encode(compressed).decode("utf-8"))

            return encoded_list

        frame = img.convert("RGB") if output_fmt == "JPEG" else img.copy()
        compressed = _compress_image_to_limit(
            frame,
            size_limit_bytes=size_limit,
        )
    return [base64.b64encode(compressed).decode("utf-8")]


def debug(*args):
    """
    输出DEBUG文件
    :param args:
    :return:
    """
    from come_context_memory.config import ROOT_DIR
    output_file = ROOT_DIR / f"DEBUG_{datetime.now():%Y-%m-%d %H-%M-%S}.json"
    save_str = ""
    for o in [args]:
        try:
            new_str = json.dumps(o, ensure_ascii=False, indent=4)
            save_str += new_str

        except Exception as E:
            print("目标对象无法序列化: ", E)
            continue

    if not save_str:
        return

    output_file.write_text(save_str, "utf-8")
    print(f"已导出文件[{output_file}]")

def backup_file(source_file: Path | str, extension: str = None, target_dir: Path | str = None, target_file: Path | str = None) -> str:
    """
    自动将文件复制一份作为备份，保留后缀名
    :param source_file: 需要备份的文件
    :param extension: 新创建的备份文件的后缀，datetime格式化的格式字符串。若无则是'原名_yyyymmdd_hhmmss_backup.拓展名'
    :param target_dir: 目标文件夹，若为空则是原文件所在的文件夹
    :param target_file: 直接指定备份文件的文件名，会覆盖之前路径以及文件名拓展
    :return: 备份文件的路径
    """
    if not source_file:
        return ""

    if not os.path.isfile(source_file):
        raise FileNotFoundError(f"指定的源文件路径不存在或不是文件：{source_file}")

    dt = datetime.now()
    if not extension:
        extension = f"_{dt:%Y%m%d_%H%M%S}_backup"

    else:
        extension = f"{dt:{extension}}"

    if not target_dir:
        target_dir = os.path.dirname(source_file)

    if target_file:
        target_dir = os.path.dirname(target_file)

    os.makedirs(target_dir, exist_ok=True)
    if target_file:
        shutil.copy(source_file, target_file)

    else:
        filename = os.path.basename(source_file)
        split_text = filename.split('.')
        main_name = ''.join(split_text[:-1])
        ex_name = filename.split('.')[-1]
        target_file = os.path.join(target_dir, f"{main_name}{extension}.{ex_name}")

        shutil.copy(source_file, target_file)

    return target_file

def parse_proxies_to_httpx(proxies: dict[str, str]) -> dict[str, str] | None:
    if not proxies:
        return None

    import httpx
    mapped_proxies = {
        "http://": proxies.get("http", "#EMPTY"),
        "https://": proxies.get("https", "#EMPTY")
    }
    mapped_proxies = {k: v for k, v in mapped_proxies.items() if v != "#EMPTY"}
    proxy_mounts = (
        {scheme: httpx.AsyncHTTPTransport(proxy=proxy) for scheme, proxy in mapped_proxies.items()}
        if mapped_proxies
        else None
    )
    return proxy_mounts

async def httpx_downloader(
        url: str,
        *,
        retry: int = 1,
        method: str = "get",
        timeout=None,
        proxy: dict = None,
        default: _T = _EmptyType(),
        errors: dict = None,
        condition: Callable[[Any], bool] = None,
        sleep: float | Callable[[], float] = None,
        **extra_parameter
) -> HttpxResponse | _T:
    """
        封装的一个httpx库请求，包含get和post
        :param url:
        :param retry: 重试次数
        :param method: get/post
        :param timeout:
        :param proxy: 代理地址 str
        :param default: 失败时返回的默认结果，为None则抛出异常
        :param errors: 对应200外不同网络状态码的字典
        :param condition: 判断成功的的额外条件
        :param sleep: 每次请求暂停的时间，用来规避反爬
        :param extra_parameter 请求的额外参数
        :return:
        """
    import httpx
    if proxy is None:
        proxy = {}

    retries = 0
    result = None
    last_except: Exception | str = "Unknown Exception"
    mounts = parse_proxies_to_httpx(proxy)
    if "proxy" in extra_parameter:
        mounts = None

    if "timeout" in extra_parameter:
        timeout = extra_parameter["timeout"]
        extra_parameter.pop("timeout")

    async with httpx.AsyncClient(mounts=mounts, timeout=timeout, **extra_parameter) as client:
        while retries < retry:
            if sleep is not None:
                if callable(sleep):
                    await asyncio.sleep(sleep())

                else:
                    await asyncio.sleep(sleep)

            try:
                if method.lower() == "get":
                    response = await client.get(url=url)

                else:
                    response = await client.post(url=url)

            except httpx.TimeoutException:
                last_except = "Timeout"

            except Exception as E:
                last_except = E

            else:
                result = response
                if result.status_code >= 300:
                    if isinstance(errors, dict) and result.status_code in errors:
                        last_except = f"status code: {result.status_code}, message: {errors[result.status_code]}"

                    else:
                        try:
                            error_body = await response.aread()
                            error_data = json.loads(error_body) if error_body else {}

                        except:
                            error_data = {}

                        error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                        last_except = f"status code: {result.status_code}, message: {error_msg}"

                    continue

                else:
                    if condition is not None:
                        try:
                            if not condition(result):
                                last_except = "Response doesn't match condition"
                                continue

                            else:
                                break

                        except Exception as E:
                            last_except = E
                            continue

                    else:
                        break

            finally:
                retries += 1

    if not result:
        if not isinstance(default, _EmptyType):
            return default

        else:
            if isinstance(last_except, str):
                raise httpx.HTTPError(last_except)

            raise last_except

    return HttpxResponse(result)

def atomic_save_json(
        content: Any,
        target_path: Path,
        time_info: bool = False,
        time_format: str = "_%y%m%d_%H%M%S",
        indent: int = 0
) -> Path | None:
    dt = datetime.now()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if time_info:
        suffix = target_path.suffix
        prefix = target_path.stem
        target_path = target_path.with_name(f"{prefix}{dt:{time_format}}{suffix}")

    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    try:
        with temp_path.open('w', encoding="utf-8") as f:
            f.write(json.dumps(content, ensure_ascii=False, indent=indent if indent else None))

    except Exception as E:
        raise E

    else:
        if temp_path.is_file():
            temp_path.replace(target_path)  # 原子替换
            return temp_path

    finally:
        try:
            temp_path.unlink(missing_ok=True)

        except:
            pass


def find_path_by_time_sequence(parent_path: Path, pattern: str | re.Pattern, time_format: str, n: int = 1, reverse=True) -> list[Path]:
    """
    查找目录下最新的n个时间序列路径
    :param parent_path:
    :param pattern: 正则表达式
    :param time_format: 时间格式化字符串
    :param n:
    :param reverse:
    :return:
    """
    if not parent_path.is_dir():
        return []

    if not isinstance(pattern, re.Pattern):
        pattern = re.compile(pattern)

    paths = os.listdir(parent_path)
    filtered_paths = []
    for path in paths:
        match = pattern.search(path)
        if match and match.group(1):
            try:
                dt = datetime.strptime(match.group(1), time_format)

            except ValueError:
                continue

            else:
                filtered_paths.append((parent_path / path, dt))

    filtered_paths.sort(key=lambda x: x[1], reverse=reverse)
    return [p[0] for i, p in enumerate(filtered_paths) if i < n]

def run_python_file(file: Path, arguments: str = "", timeout: int = 300) -> dict:
    """
    运行 Python 文件。
    """
    import subprocess
    import sys

    if not file.is_file():
        raise FileNotFoundError(f"Python file not found: {file}")

    if file.suffix.lower() != ".py":
        raise ValueError(f"Not a python file: {file}")

    completed = subprocess.run(
        [sys.executable, str(file)] + arguments.split(' '),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

    result_obj = None
    stdout = completed.stdout or ""
    if stdout.strip():
        # 尝试解析最后一行 JSON 作为 result
        lines = [ln for ln in stdout.splitlines() if ln.strip()]
        if lines:
            try:
                result_obj = json.loads(lines[-1])

            except Exception:
                result_obj = None

    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "result": result_obj,
    }

def timestamp2text(timestamp: float, _format: str = "%y-%m-%d %H:%M:%S"):
    dt = datetime.fromtimestamp(timestamp)
    return f"{dt:{_format}}"
