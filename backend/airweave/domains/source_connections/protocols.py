"""Protocols for source connection domain."""

from datetime import datetime
from typing import Any, Dict, Optional, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.domains.source_connections.types import SourceConnectionStats
from airweave.models.connection_init_session import ConnectionInitSession
from airweave.models.source_connection import SourceConnection
from airweave.models.sync_job import SyncJob
from airweave.schemas.source_connection import (
    SourceConnection as SourceConnectionSchema,
)
from airweave.schemas.source_connection import (
    SourceConnectionJob,
    SourceConnectionListItem,
)


class SourceConnectionRepositoryProtocol(Protocol):
    """Data access for source connections.

    Wraps crud.source_connection for testability.
    """

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        """Get a source connection by ID within org scope."""
        ...

    async def get_schedule_info(
        self, db: AsyncSession, source_connection: SourceConnection
    ) -> Optional[Dict[str, Any]]:
        """Get schedule info for a source connection."""
        ...

    async def get_init_session_with_redirect(
        self, db: AsyncSession, session_id: UUID, ctx: ApiContext
    ) -> Optional[ConnectionInitSession]:
        """Get a ConnectionInitSession by ID with redirect_session eagerly loaded."""
        ...


class ResponseBuilderProtocol(Protocol):
    """Builds API response schemas for source connections."""

    async def build_response(
        self,
        db: AsyncSession,
        source_conn: SourceConnection,
        ctx: ApiContext,
        *,
        auth_url_override: Optional[str] = None,
        auth_url_expiry_override: Optional[datetime] = None,
    ) -> SourceConnectionSchema:
        """Build full SourceConnection response from ORM object."""
        ...

    def build_list_item(self, stats: SourceConnectionStats) -> SourceConnectionListItem:
        """Build a SourceConnectionListItem from a typed stats object."""
        ...

    def map_sync_job(self, job: SyncJob, source_connection_id: UUID) -> SourceConnectionJob:
        """Convert sync job to SourceConnectionJob schema."""
        ...
