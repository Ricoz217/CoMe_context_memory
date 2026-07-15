from __future__ import annotations

import ast
import unittest
from pathlib import Path


MEMORY_DIR = Path(__file__).resolve().parents[1] / "src" / "context_memory" / "memory"
BUSINESS_FILES = [MEMORY_DIR / "engine.py", *sorted((MEMORY_DIR / "services").glob("*.py"))]
COMPATIBILITY_METHODS = {
    "get_or_create_alias",
    "resolve_alias",
    "freeze_alias_map",
    "build_llm_view",
    "resolve_llm_output",
    "alias_map_version",
    "assert_alias_only_payload",
}
FORBIDDEN_METHODS = {
    "build_llm_view",
    "resolve_llm_output",
    "get_or_create_alias",
    "resolve_alias",
    "freeze_alias_map",
    "alias_map_version",
    "assert_alias_only_payload",
}
FORBIDDEN_STORAGE_METHODS = {
    "find_alias",
    "resolve_alias",
    "get_or_create_alias",
    "freeze_alias_map",
    "load_alias_map",
    "save_alias_map",
    "commit_alias_map",
}


def _attribute_path(node: ast.AST) -> str:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


class _LegacyAliasCallVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.function_stack: list[str] = []
        self.findings: list[tuple[int, str]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        if node.name in COMPATIBILITY_METHODS:
            return
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        path = _attribute_path(node.func)
        method = path.rsplit(".", 1)[-1]
        if method in FORBIDDEN_METHODS:
            self.findings.append((node.lineno, path))
        if ".storage." in f".{path}." and method in FORBIDDEN_STORAGE_METHODS:
            self.findings.append((node.lineno, path))
        self.generic_visit(node)


class BusinessAliasMigrationTests(unittest.TestCase):
    def test_business_code_does_not_call_legacy_alias_facades_or_storage_maps(self) -> None:
        findings: list[str] = []
        for path in BUSINESS_FILES:
            visitor = _LegacyAliasCallVisitor()
            visitor.visit(ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path)))
            findings.extend(f"{path.relative_to(MEMORY_DIR)}:{line}: {call}" for line, call in visitor.findings)

        self.assertEqual(findings, [], "legacy alias calls remain in business code:\n" + "\n".join(findings))


if __name__ == "__main__":
    unittest.main()
