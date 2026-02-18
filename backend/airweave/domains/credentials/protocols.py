"""Protocols for integration credential repository."""

from typing import Optional, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.models.integration_credential import IntegrationCredential


class IntegrationCredentialRepositoryProtocol(Protocol):
    """Read-only access to integration credential records."""

    async def get(
        self, db: AsyncSession, id: UUID, ctx: ApiContext
    ) -> Optional[IntegrationCredential]:
        """Get an integration credential by ID within an organization."""
        ...
