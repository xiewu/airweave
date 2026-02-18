"""Integration credential repository wrapping crud.integration_credential."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.api.context import ApiContext
from airweave.domains.credentials.protocols import (
    IntegrationCredentialRepositoryProtocol,
)
from airweave.models.integration_credential import IntegrationCredential


class IntegrationCredentialRepository(IntegrationCredentialRepositoryProtocol):
    """Delegates to the crud.integration_credential singleton."""

    async def get(
        self, db: AsyncSession, id: UUID, ctx: ApiContext
    ) -> Optional[IntegrationCredential]:
        return await crud.integration_credential.get(db, id, ctx)
