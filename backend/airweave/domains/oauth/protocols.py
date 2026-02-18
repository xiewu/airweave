"""Protocols for OAuth2 token operations."""

from typing import Optional, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.platform.auth.schemas import OAuth2TokenResponse


class OAuth2ServiceProtocol(Protocol):
    """OAuth2 token refresh capability."""

    async def refresh_access_token(
        self,
        db: AsyncSession,
        integration_short_name: str,
        ctx: ApiContext,
        connection_id: UUID,
        decrypted_credential: dict,
        config_fields: Optional[dict] = None,
    ) -> OAuth2TokenResponse:
        """Refresh an OAuth2 access token.

        Returns an OAuth2TokenResponse with an `access_token` attribute.
        """
        ...
