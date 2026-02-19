"""Protocols for the syncs domain."""

from typing import Optional, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.models.sync_job import SyncJob


class SyncJobRepositoryProtocol(Protocol):
    """Read-only access to sync job records."""

    async def get_latest_by_sync_id(self, db: AsyncSession, sync_id: UUID) -> Optional[SyncJob]:
        """Get the most recent sync job for a sync."""
        ...
