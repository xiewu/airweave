"""Sync job repository wrapping crud.sync_job."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.domains.syncs.protocols import SyncJobRepositoryProtocol
from airweave.models.sync_job import SyncJob


class SyncJobRepository(SyncJobRepositoryProtocol):
    """Delegates to the crud.sync_job singleton."""

    async def get_latest_by_sync_id(self, db: AsyncSession, sync_id: UUID) -> Optional[SyncJob]:
        """Get the most recent sync job for a sync."""
        return await crud.sync_job.get_latest_by_sync_id(db, sync_id=sync_id)
