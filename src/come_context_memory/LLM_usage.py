from __future__ import annotations

import time
import json
from typing import Literal, Any, Annotated, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from hashlib import blake2b
from copy import deepcopy
from come_context_memory.utils import atomic_save_json
from come_context_memory.logger import get_logger


_status = get_logger().status()

@dataclass(slots=True)
class ApiPrice:
    """token单价, currency / 1M tokens"""
    input: float = 0
    output: float = 0
    cached: float = 0
    currency: str = "CNY"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return ApiPrice(
            input=data.get("input", 0),
            output=data.get("output", 0),
            cached=data.get("cached", 0),
            currency=data.get("currency", "CNY")
        )

    def copy(self):
        return self.__copy__()

    def __bool__(self):
        return bool(self.input or self.output or self.cached)

    def __copy__(self):
        return ApiPrice(
            self.input,
            self.output,
            self.cached,
            self.currency
        )

@dataclass(slots=True)
class UsageStorage:
    model: str
    price: ApiPrice | None
    usage: dict[str, dict[str, int]] = field(
        default_factory=lambda : {
            "input": {
                "estimate": 0,
                "verified": 0
            },
            "output": {
                "estimate": 0,
                "verified": 0
            },
            "cached": {
                "estimate": 0,
                "verified": 0
            }
        }
    )
    last_update: float = field(default_factory=time.time)

    def hash_id(self) -> str:
        data = {
            "model": self.model,
            "price": self.price.to_dict() if self.price else None
        }
        key = json.dumps(data, ensure_ascii=False).encode()
        h = blake2b(key, digest_size=16)
        return h.hexdigest()

    def to_dict(self):
        return {
            "model": self.model,
            "price": self.price.to_dict() if self.price else None,
            "usage": self.usage,
            "last_update": self.last_update
        }

    @property
    def input_t(self) -> dict[str, int]:
        return self.usage["input"]

    @property
    def output_t(self) -> dict[str, int]:
        return self.usage["output"]

    @property
    def cached_t(self) -> dict[str, int]:
        return self.usage["cached"]

    @property
    def total_t(self) -> dict[str, int]:
        return {
            "estimate": self.input_t["estimate"] + self.output_t["estimate"],
            "verified": self.input_t["verified"] + self.output_t["verified"]
        }

    # 已弃用
    def __sub__(self, other) -> UsageStorage:
        """Return a delta usage"""
        if not isinstance(other, UsageStorage):
            raise TypeError

        if self.model != other.model:
            raise ValueError("Different model name")

        if self.price != other.price:
            raise ValueError("Different price")

        new_data = {
            "input": {
                "estimate": 0,
                "verified": 0
            },
            "output": {
                "estimate": self.output_t["estimate"],
                "verified": self.output_t["verified"]
            },
            "cached": {
                "estimate": 0,
                "verified": 0
            }
        }

        #input
        if self.input_t["estimate"] and self.input_t["verified"]:
            new_data["input"]["estimate"] = self.input_t["estimate"] - other.input_t["estimate"]
            new_data["input"]["verified"] = self.input_t["verified"] - other.input_t["verified"]

        else:
            if self.input_t["estimate"]:
                new_data["input"]["estimate"] = sum(self.input_t.values())
                new_data["input"]["verified"] = -sum(other.input_t.values())

            elif self.input_t["verified"]:
                new_data["input"]["verified"] = sum(self.input_t.values())
                new_data["input"]["estimate"] = -sum(other.input_t.values())

        # cached
        if self.cached_t["estimate"] and self.cached_t["verified"]:
            new_data["cached"]["estimate"] = self.cached_t["estimate"] - other.cached_t["estimate"]
            new_data["cached"]["verified"] = self.cached_t["verified"] - other.cached_t["verified"]

        else:
            cached_t = sum(self.cached_t.values()) - sum(other.cached_t.values())
            if self.cached_t["estimate"]:
                new_data["cached"]["estimate"] = sum(self.cached_t.values())
                new_data["cached"]["verified"] = -sum(other.cached_t.values())

            elif self.cached_t["verified"]:
                new_data["cached"]["verified"] = sum(self.cached_t.values())
                new_data["cached"]["estimate"] = -sum(other.cached_t.values())

        return UsageStorage(self.model, self.price, new_data)

    def __copy__(self):
        return UsageStorage(
            self.model,
            self.price.copy(),
            deepcopy(self.usage),
            self.last_update
        )

    # 已弃用
    @staticmethod
    def _redistribute(data: dict):
        """
        再分配token，不能低于0，同时更改可信标记
        :param data:
        :return:
        """
        count = sum(data.values())
        if count < 0:
            raise ValueError("Usage below zero")

        if data["estimate"] < 0:
            data["verified"] = count
            data["estimate"] = 0

        elif data["verified"] < 0:
            data["estimate"] = count
            data["verified"] = 0

    # 已弃用
    def normalize(self):
        self._redistribute(self.input_t)
        self._redistribute(self.output_t)
        self._redistribute(self.cached_t)

    def add(self, other: UsageStorage):
        if not isinstance(other, UsageStorage):
            raise TypeError

        if self.model != other.model:
            raise ValueError("Different model name")

        if self.price != other.price:
            raise ValueError("Different price")

        # input
        self.input_t["estimate"] += other.input_t["estimate"]
        self.input_t["verified"] += other.input_t["verified"]

        # output
        self.output_t["estimate"] += other.output_t["estimate"]
        self.output_t["verified"] += other.output_t["verified"]

        # cached
        self.cached_t["estimate"] += other.cached_t["estimate"]
        self.cached_t["verified"] += other.cached_t["verified"]

        self.last_update = time.time()

    def copy(self):
        return self.__copy__()

    @classmethod
    def from_dict(cls, data: dict):
        price = data.get("price", {})
        return UsageStorage(
            model=data["model"],
            price=ApiPrice.from_dict(price) if price is not None else None,
            usage=data.get("usage", {
                "input": {
                    "estimate": 0,
                    "verified": 0
                },
                "output": {
                    "estimate": 0,
                    "verified": 0
                },
                "cached": {
                    "estimate": 0,
                    "verified": 0
                }
            }),
            last_update=data.get("last_update", time.time())
        )


class LLMUsage:
    def __init__(self, data_file: Path | None, update_callback: Callable = None):
        if _GLOBAL_FILE is not None and data_file == _GLOBAL_FILE:
            raise PermissionError("Can not use global file in sub instance")

        self.data_file = Path(data_file) if data_file else None
        self.callback = update_callback
        self._storage: dict[str, dict[str, dict[str, dict[str, UsageStorage]]]] = {}
        self._last_record: dict[str, UsageStorage] = {}

    def to_dict(self) -> dict:
        def _decode(data: dict):
            for k, v in data.items():
                if isinstance(v, dict):
                    _decode(v)

                elif isinstance(v, UsageStorage):
                    data[k] = v.to_dict()

        storage_mirror = deepcopy(self._storage)
        _decode(storage_mirror)
        return {
            "last_record": {k: v.to_dict() for k, v in self._last_record.items()},
            "storage": storage_mirror
        }

    def save(self):
        if self.data_file is None:
            return
        atomic_save_json(self.to_dict(), self.data_file, indent=4)

    def load_dict(self, data: dict):
        storage = {}
        last_record = data.pop("last_record", {})
        for k, v in last_record.items():
            try:
                usage = UsageStorage.from_dict(v)

            except KeyError:
                continue

            else:
                self._last_record[k] = usage

        for year, content_1 in data.get("storage", {}).items():
            storage[year] = {}
            for month, content_2 in content_1.items():
                storage[year][month] = {}
                for day, content_3 in content_2.items():
                    storage[year][month][day] = {}
                    for key, content_usage in content_3.items():
                        try:
                            storage[year][month][day][key] = UsageStorage.from_dict(content_usage)

                        except KeyError:
                            continue

        self._storage.update(storage)

    def load(self):
        if self.data_file is None:
            return
        if not self._storage:
            if self.data_file.is_file():
                try:
                    load_content = json.loads(self.data_file.read_text(encoding="utf-8"))

                except json.JSONDecodeError:
                    return

                else:
                    self.load_dict(load_content)

    @staticmethod
    def get_key(model: str, price: ApiPrice = None):
        data = {
            "model": model,
            "price": price.to_dict() if price else None
        }
        key = json.dumps(data, ensure_ascii=False).encode()
        h = blake2b(key, digest_size=16)
        return h.hexdigest()

    def update(self, model: str, update_data: dict[str, int], price: ApiPrice = None, verified: bool = True):
        """
        自动更新token记录
        :param model: 模型名
        :param update_data: {"input": int, "output": int, "cached": int}
        :param price: ApiPrice
        :param verified: 是否估算
        :return:
        """
        if not self._storage:
            self.load()

        input_t = update_data.get("input", 0)
        output_t = update_data.get("output", 0)
        cached = update_data.get("cached", 0)

        new_usage = UsageStorage(model, price)
        if verified:
            new_usage.usage["input"]["verified"] = input_t
            new_usage.usage["output"]["verified"] = output_t
            new_usage.usage["cached"]["verified"] = cached

        else:
            new_usage.usage["input"]["estimate"] = input_t
            new_usage.usage["output"]["estimate"] = output_t
            new_usage.usage["cached"]["estimate"] = cached

        now = datetime.fromtimestamp(new_usage.last_update)
        hash_key = new_usage.hash_id()
        day_storage = self._storage.setdefault(str(now.year), {}).setdefault(str(now.month), {}).setdefault(
            str(now.day), {})
        if hash_key in day_storage:
            day_storage[hash_key].add(new_usage)

        else:
            day_storage[hash_key] = new_usage

        self._last_record[hash_key] = new_usage
        self.save()
        if callable(self.callback):
            self.callback()

    def overview(self, period: Literal["YEAR", "MONTH", "DAY", "ALL"] = "MONTH") -> dict:
        """
        返回用量概览，不区分模型
        :param period:
        :return:
        """
        if not self._storage:
            self.load()

        def _sum_period(_data: dict):
            for _k, _v in _data.items():
                if isinstance(_v, dict):
                    _sum_period(_v)

                elif isinstance(_v, UsageStorage):
                    result["input"]["estimate"] += _v.input_t["estimate"]
                    result["input"]["verified"] += _v.input_t["verified"]
                    result["output"]["estimate"] += _v.output_t["estimate"]
                    result["output"]["verified"] += _v.output_t["verified"]
                    result["cached"]["estimate"] += _v.cached_t["estimate"]
                    result["cached"]["verified"] += _v.cached_t["verified"]

                    if _v.price:
                        result["cost"][_v.price.currency] = result["cost"].get(_v.price.currency, 0.0) + (
                                    sum(_v.input_t.values()) - sum(_v.cached_t.values())) * _v.price.input / 1_000_000
                        result["cost"][_v.price.currency] += sum(_v.cached_t.values()) * (
                                    _v.price.cached or _v.price.input) / 1_000_000
                        result["cost"][_v.price.currency] += sum(_v.output_t.values()) * _v.price.output / 1_000_000

        result: dict[str, dict[str, Any]] = {
            "input": {
                "estimate": 0,
                "verified": 0
            },
            "output": {
                "estimate": 0,
                "verified": 0
            },
            "cached": {
                "estimate": 0,
                "verified": 0
            },
            "cost": {}
        }
        now = datetime.now()
        y, m, d = str(now.year), str(now.month), str(now.day)
        if period == "ALL":
            _sum_period(self._storage)

        elif period == "YEAR":
            if y in self._storage:
                _sum_period(self._storage[y])

        elif period == "MONTH":
            if y in self._storage and m in self._storage[y]:
                _sum_period(self._storage[y][m])

        elif period == "DAY":
            if y in self._storage and m in self._storage[y] and d in self._storage[y][m]:
                _sum_period(self._storage[y][m][d])

        return result

    def query(
            self,
            hash_key: str = "",
            time_window: float | tuple[float, float] = None
    ) -> dict[Annotated[str, "year"], dict[
        Annotated[str, "month"], dict[Annotated[str, "day"], dict[Annotated[str, "hash_key"], UsageStorage]]]]:
        """
        查询用量，按天划分
        :param hash_key: 不给就查全部
        :param time_window: 时间窗口
        :return: 嵌套字典
        """
        output = {}
        dts: list[datetime] = []
        if time_window is None:
            dts.append(datetime.now())

        elif isinstance(time_window, float):
            dts.append(datetime.fromtimestamp(time_window))

        elif isinstance(time_window, tuple):
            for ts in time_window:
                dts.append(datetime.fromtimestamp(ts))

        else:
            raise TypeError

        if len(dts) > 1:
            start_y, start_m, start_d = dts[0].year, dts[0].month, dts[0].day
            end_y, end_m, end_d = dts[1].year, dts[1].month, dts[1].day

        else:
            start_y, start_m, start_d = dts[0].year, dts[0].month, dts[0].day
            end_y, end_m, end_d = start_y, start_m, start_d

        for year, content_1 in self._storage.items():
            if int(year) < start_y or 0 < end_y < int(year):
                continue

            for month, content_2 in content_1.items():
                if int(month) < start_m or 0 < end_m < int(month):
                    continue

                for day, content_3 in content_2.items():
                    if int(day) < start_d or 0 < end_d < int(day):
                        continue

                    for k, v in content_3.items():
                        if hash_key:
                            if k != hash_key:
                                continue

                        output.setdefault(year, {}).setdefault(month, {}).setdefault(day, {})[k] = v.copy()

        return output


_GLOBAL_FILE: Path | None = None
_GLOBAL_USAGE = LLMUsage(None)
def _auto_flush():
    usage = _GLOBAL_USAGE.overview()
    input_t = sum(usage.get("input", {}).values())
    output_t = sum(usage.get("output", {}).values())
    cached_t = sum(usage.get("cached", {}).values())
    cost = {k: v for k, v in usage.get("cost", {}).items() if k in {"CNY", "USD"}}
    output = "本月Token消耗 | "
    output += f"总: [{input_t + output_t:,}]; "
    output += f"输入: [{input_t:,}]; "
    output += f"输出: [{output_t:,}]; "
    output += f"输入(命中缓存): [{cached_t:,}]; "
    output += f"缓存命中率: [{cached_t/input_t if input_t else 0:.2%}]"

    if cost:
        output += " | "
        if CNY:= cost.get("CNY", 0):
            output += f"CNY: [{CNY:,.2f}]"

        if USD := cost.get("USD", 0):
            output += f"USD: [{USD:,.2f}]"

    _status.update(output)

_GLOBAL_USAGE.callback = _auto_flush




