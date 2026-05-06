"""
file_cache.py
文件缓存系统，提供基础接口
"""
import hashlib
import base64
import time
import json
import threading
from pathlib import Path
from io import BytesIO
from dataclasses import dataclass, field
from context_memory.config import ROOT_DIR, SETTING_CFG
from context_memory.utils import atomic_save_json

_CACHE_PATH = ROOT_DIR / "data" / "file_cache"
_METADATA_FILE = _CACHE_PATH / "metadata.json"
_LOCK = threading.Lock()


def configure_global_file_cache_dir(cache_dir: str | Path) -> None:
    global _CACHE_PATH, _METADATA_FILE
    new_cache = Path(cache_dir)
    with _LOCK:
        _CACHE_PATH = new_cache
        _METADATA_FILE = _CACHE_PATH / "metadata.json"


@dataclass(slots=True)
class FileInfo:
    create_time: str = field(default_factory=lambda : str(time.time()))
    use_time: str = field(default_factory=lambda : str(time.time()))
    file_type: str = "file"
    note: str = ""

    def to_dict(self):
        return {
            "create_time": self.create_time,
            "use_time": self.use_time,
            "file_type": self.file_type,
            "note": self.note
        }

    @classmethod
    def from_dict(cls, data: dict):
        return FileInfo(**data)


def _clear_expire_file():
    with _LOCK:
        metadata: dict = json.loads(_METADATA_FILE.read_text(encoding="utf-8"))
        metadata.pop("last_clear", None)
        expire_files = []
        expire = SETTING_CFG.Common.FileCacheExpire * 24 * 3600
        now = time.time()
        for k, v in metadata.items():
            try:
                file_info = FileInfo.from_dict(v)

            except:
                continue

            else:
                if now - float(file_info.use_time) > expire:
                    expire_files.append(k)

        for name in expire_files:
            path = _CACHE_PATH / name[:2] / name
            if path.is_file():
                try:
                    path.unlink()

                except:
                    pass

                else:
                    metadata.pop(name)

        metadata["last_clear"] = str(time.time())
        atomic_save_json(metadata, _METADATA_FILE)

def _update_metadata(name: str, new_data: FileInfo = None, miss_ok=True):
    with _LOCK:
        if _METADATA_FILE.is_file():
            metadata: dict = json.loads(_METADATA_FILE.read_text(encoding="utf-8"))

        else:
            metadata = {}

        if new_data is None:
            if name in metadata:
                metadata[name]["use_time"] = str(time.time())

            elif miss_ok:
                metadata[name] = FileInfo().to_dict()

            else:
                raise FileNotFoundError(f"hash name: [{name}]")

        else:
            if name in metadata:
                new_data.create_time = metadata[name]["create_time"]

            metadata[name] = new_data.to_dict()

        atomic_save_json(metadata, _METADATA_FILE)

    last_clear = metadata.get("last_clear", time.time())
    last_clear = float(last_clear)
    if time.time() - last_clear > 24 * 3600:
        t = threading.Thread(target=_clear_expire_file, daemon=True)
        t.start()

def renew(hash_name: str):
    try:
        _update_metadata(hash_name, miss_ok=False)

    except FileNotFoundError:
        pass

def get_file_path(hash_name: str) -> Path:
    """
    通过哈希名返回文件
    """
    if len(hash_name) < 3:
        raise ValueError("文件名不正确")

    target_file = _CACHE_PATH / hash_name[:2] / hash_name
    if not target_file.is_file():
        raise FileNotFoundError

    _update_metadata(hash_name)
    return target_file

def add_file(file: str | Path | bytes | BytesIO, file_type: str = "file", note: str = "") -> str:
    """
    将文件存入缓存，并返回哈希文件名
    :param file: 若传入是字符串，必须是base64字符串，而不是文件路径，文件路径只允许Path方式传入
    :param file_type:
    :param note:
    :return: 返回的是哈希名字字符串
    """
    if isinstance(file, str):
        data = base64.b64decode(file)

    elif isinstance(file, Path):
        data = file.read_bytes()

    elif isinstance(file, bytes):
        data = file

    elif hasattr(file, "read") and hasattr(file, "seek"):
        stream = file
        current_pos = stream.tell()
        stream.seek(0)
        data = stream.read()
        stream.seek(current_pos)

    else:
        raise TypeError("不支持该类型的文件")

    if not isinstance(data, bytes):
        raise TypeError("文件无法读取成bytes")

    h = hashlib.blake2b(data, digest_size=16)
    file_name = h.hexdigest()
    file_path = _CACHE_PATH / file_name[:2] / file_name
    file_path.parent.mkdir(exist_ok=True, parents=True)

    # atomic write
    file_tmp = file_path.with_name(f"{file_path.name}.tmp")
    file_tmp.write_bytes(data)
    file_tmp.replace(file_path)

    try:
        file_tmp.unlink()

    except:
        pass

    file_info = FileInfo(file_type=file_type, note=note)
    _update_metadata(file_name, file_info)
    return file_name

def remove_fire(file: str | Path):
    if isinstance(file, Path):
        name = file.name

    else:
        name = file

    with _LOCK:
        metadata: dict = json.loads(_METADATA_FILE.read_text(encoding="utf-8"))
        metadata.pop(name, None)
        atomic_save_json(metadata, _METADATA_FILE)

    path = _CACHE_PATH / name[:2] / name
    if not path.exists():
        return

    try:
        path.unlink()

    except:
        return
