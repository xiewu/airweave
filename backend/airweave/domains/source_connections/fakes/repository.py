"""Fake source connection repository for testing."""

from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.models.connection_init_session import ConnectionInitSession
from airweave.models.source_connection import SourceConnection


class FakeSourceConnectionRepository:
    """In-memory fake for SourceConnectionRepositoryProtocol."""

    def __init__(self) -> None:
        """Initialize with empty stores."""
        self._store: dict[UUID, SourceConnection] = {}
        self._schedule_info: dict[UUID, Dict[str, Any]] = {}
        self._init_sessions: dict[UUID, ConnectionInitSession] = {}
        self._calls: list[tuple] = []

    def seed(self, id: UUID, obj: SourceConnection) -> None:
        """Seed a source connection by ID."""
        self._store[id] = obj

    def seed_schedule_info(self, sc_id: UUID, info: Dict[str, Any]) -> None:
        """Seed schedule info keyed by source connection ID."""
        self._schedule_info[sc_id] = info

    def seed_init_session(self, session_id: UUID, obj: ConnectionInitSession) -> None:
        """Seed a ConnectionInitSession by ID."""
        self._init_sessions[session_id] = obj

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        """Return seeded connection or None."""
        self._calls.append(("get", db, id, ctx))
        return self._store.get(id)

    async def get_schedule_info(
        self, db: AsyncSession, source_connection: SourceConnection
    ) -> Optional[Dict[str, Any]]:
        """Return seeded schedule info or None."""
        self._calls.append(("get_schedule_info", db, source_connection))
        return self._schedule_info.get(source_connection.id)

    async def get_init_session_with_redirect(
        self, db: AsyncSession, session_id: UUID, ctx: ApiContext
    ) -> Optional[ConnectionInitSession]:
        """Return seeded init session or None."""
        self._calls.append(("get_init_session_with_redirect", db, session_id, ctx))
        return self._init_sessions.get(session_id)
