"""Automated architectural test asserting strict model isolation for the API service.

Asserts that no code within services/api imports from:
  - services.postgres.models
  - services.consumer
  - services.snowflake_adapter
"""

from __future__ import annotations

import ast
from pathlib import Path


def test_api_models_strict_isolation() -> None:
    """Ensure services/api never imports models or code from other services."""
    api_root = Path(__file__).resolve().parent.parent

    forbidden_modules = [
        "services.postgres.models",
        "services.consumer",
        "services.snowflake_adapter",
    ]

    violations: list[str] = []

    for py_file in api_root.rglob("*.py"):
        # Skip the isolation test itself
        if py_file.name == "test_models_isolation.py":
            continue

        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for forbidden in forbidden_modules:
                        if alias.name.startswith(forbidden):
                            violations.append(
                                f"{py_file.relative_to(api_root.parent.parent)}: "
                                f"Forbidden import '{alias.name}' (line {node.lineno})"
                            )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    for forbidden in forbidden_modules:
                        if node.module.startswith(forbidden):
                            violations.append(
                                f"{py_file.relative_to(api_root.parent.parent)}: "
                                f"Forbidden import from '{node.module}' (line {node.lineno})"
                            )

    assert not violations, (
        "Model isolation violation detected! The API service must remain independent "
        "and not import from other services:\n" + "\n".join(violations)
    )
