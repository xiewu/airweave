"""Fake sync job repository for testing."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.models.sync_job import SyncJob


class FakeSyncJobRepository:
    """In-memory fake for SyncJobRepositoryProtocol."""

    def __init__(self) -> None:
        """Initialize with empty store."""
        self._last_jobs: dict[UUID, SyncJob] = {}
        self._calls: list[tuple] = []

    def seed_last_job(self, sync_id: UUID, job: SyncJob) -> None:
        """Seed a last job for a given sync_id."""
        self._last_jobs[sync_id] = job

    async def get_latest_by_sync_id(self, db: AsyncSession, sync_id: UUID) -> Optional[SyncJob]:
        """Return seeded job or None."""
        self._calls.append(("get_latest_by_sync_id", db, sync_id))
        return self._last_jobs.get(sync_id)
