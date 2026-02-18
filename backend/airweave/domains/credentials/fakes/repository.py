"""Fake integration credential repository for testing."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.models.integration_credential import IntegrationCredential


class FakeIntegrationCredentialRepository:
    """In-memory fake for IntegrationCredentialRepositoryProtocol."""

    def __init__(self) -> None:
        self._store: dict[UUID, IntegrationCredential] = {}
        self._calls: list[tuple] = []

    def seed(self, id: UUID, obj: IntegrationCredential) -> None:
        self._store[id] = obj

    async def get(
        self, db: AsyncSession, id: UUID, ctx: ApiContext
    ) -> Optional[IntegrationCredential]:
        self._calls.append(("get", db, id, ctx))
        return self._store.get(id)
