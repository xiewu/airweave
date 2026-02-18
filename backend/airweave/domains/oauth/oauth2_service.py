"""OAuth2 service wrapping the platform oauth2_service singleton."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.domains.oauth.protocols import OAuth2ServiceProtocol
from airweave.platform.auth.oauth2_service import oauth2_service
from airweave.platform.auth.schemas import OAuth2TokenResponse


class OAuth2Service(OAuth2ServiceProtocol):
    """Delegates to the oauth2_service module-level singleton."""

    async def refresh_access_token(
        self,
        db: AsyncSession,
        integration_short_name: str,
        ctx: ApiContext,
        connection_id: UUID,
        decrypted_credential: dict,
        config_fields: Optional[dict] = None,
    ) -> OAuth2TokenResponse:
        return await oauth2_service.refresh_access_token(
            db,
            integration_short_name,
            ctx,
            connection_id,
            decrypted_credential,
            config_fields,
        )
