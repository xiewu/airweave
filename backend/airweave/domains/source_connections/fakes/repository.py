"""Fake source connection repository for testing."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.models.source_connection import SourceConnection


class FakeSourceConnectionRepository:
    """In-memory fake for SourceConnectionRepositoryProtocol."""

    def __init__(self) -> None:
        self._store: dict[UUID, SourceConnection] = {}
        self._calls: list[tuple] = []

    def seed(self, id: UUID, obj: SourceConnection) -> None:
        self._store[id] = obj

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        self._calls.append(("get", db, id, ctx))
        return self._store.get(id)
