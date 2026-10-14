"""FastAPI dependency injection providers."""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from fastapi import Depends

from services.api.api.config import APISettings
from services.api.api.db.repository import APIRepository

_repo_override: Optional[APIRepository] = None


@lru_cache()
def get_api_settings() -> APISettings:
    """Provide cached APISettings singleton."""
    return APISettings()


def set_repository_override(repo: Optional[APIRepository]) -> None:
    """Override repository instance (primarily for testing)."""
    global _repo_override
    _repo_override = repo


def get_repository(
    settings: APISettings = Depends(get_api_settings),
) -> APIRepository:
    """Provide APIRepository dependency for route handlers."""
    if _repo_override is not None:
        return _repo_override
    return APIRepository(settings=settings)
