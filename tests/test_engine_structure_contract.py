from __future__ import annotations

import ast
import hashlib
import inspect
import json
from dataclasses import MISSING, fields
from pathlib import Path

from context_memory.memory import BucketHandle, ContextMemoryConfig, ContextMemoryEngineV3
from context_memory.memory import engine as engine_module


ENGINE_SIGNATURE_HASH = "919b8626763ce7f65c41a20adb5116c8301fb4b546c9e4f90286d9da501751dd"
HANDLE_SIGNATURE_HASH = "028083d206d611d46d82c5259555fc704f8745eb1bdd3eebbd921241f99123f2"
CONFIG_FIELDS_HASH = "fee6b323495d2e4e1cca48ad0da3a6692395707fb340911bdcf7bc6d1b63b406"


def _public_signatures(cls: type) -> dict[str, str]:
    signatures = {"__init__": str(inspect.signature(cls))}
    signatures.update(
        {
            name: str(inspect.signature(value))
            for name, value in cls.__dict__.items()
            if callable(value) and not name.startswith("_")
        }
    )
    return signatures


def _stable_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_public_engine_handle_and_config_contracts_are_stable() -> None:
    config_fields: list[tuple[str, str]] = []
    for field in fields(ContextMemoryConfig):
        if field.default is not MISSING:
            default = repr(field.default)
        elif field.default_factory is not MISSING:
            default = "<factory>"
        else:
            default = "<missing>"
        config_fields.append((field.name, default))

    assert _stable_hash(_public_signatures(ContextMemoryEngineV3)) == ENGINE_SIGNATURE_HASH
    assert _stable_hash(_public_signatures(BucketHandle)) == HANDLE_SIGNATURE_HASH
    assert _stable_hash(config_fields) == CONFIG_FIELDS_HASH


def test_public_compatibility_exports_keep_type_identity() -> None:
    from context_memory.memory.bucket_handle import BucketHandle as ExtractedBucketHandle
    from context_memory.memory.config import ContextMemoryConfig as ExtractedConfig

    assert BucketHandle is ExtractedBucketHandle
    assert ContextMemoryConfig is ExtractedConfig
    assert engine_module.BucketHandle is ExtractedBucketHandle
    assert engine_module.ContextMemoryConfig is ExtractedConfig


def test_engine_and_services_respect_architecture_boundaries() -> None:
    memory_root = Path(engine_module.__file__).resolve().parent
    engine_path = memory_root / "engine.py"
    runtime_path = memory_root / "engine_runtime.py"
    source = engine_path.read_text(encoding="utf-8-sig")
    tree = ast.parse(source)

    assert len(source.splitlines()) <= 1200
    assert len(runtime_path.read_text(encoding="utf-8-sig").splitlines()) <= 600
    assert not (memory_root / "services" / "runtime.py").exists()
    class_names = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    assert "BucketHandle" not in class_names
    assert "ContextMemoryConfig" not in class_names

    violations: list[str] = []
    architecture_paths = [
        engine_path,
        runtime_path,
        *sorted((memory_root / "services").glob("*.py")),
    ]
    for path in architecture_paths:
        service_source = path.read_text(encoding="utf-8-sig")
        if "ServiceRuntime" in service_source:
            violations.append(f"{path.name}: ServiceRuntime")
        if "runtime.engine" in service_source or ".runtime.engine" in service_source:
            violations.append(f"{path.name}: runtime.engine")
        service_tree = ast.parse(service_source)
        for node in ast.walk(service_tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "__getattr__"
            ):
                violations.append(f"{path.name}:{node.lineno}: __getattr__")
            if (
                path.parent.name == "services"
                and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.end_lineno is not None
                and node.end_lineno - node.lineno + 1 > 200
            ):
                violations.append(
                    f"{path.name}:{node.lineno}: {node.name} exceeds 200 lines"
                )
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "eng"
                and node.attr.startswith("_")
            ):
                violations.append(f"{path.name}:{node.lineno}: eng.{node.attr}")

    assert violations == []
