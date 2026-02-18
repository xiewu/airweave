"""Fake OAuth2 service for testing."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext


class FakeOAuth2TokenResponse:
    """Minimal token response returned by FakeOAuth2Service."""

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token


class FakeOAuth2Service:
    """In-memory fake for OAuth2ServiceProtocol."""

    def __init__(self) -> None:
        self._responses: dict[str, FakeOAuth2TokenResponse] = {}
        self._calls: list[tuple] = []
        self._should_raise: Optional[Exception] = None

    def seed(self, integration_short_name: str, access_token: str) -> None:
        self._responses[integration_short_name] = FakeOAuth2TokenResponse(access_token)

    def set_error(self, error: Exception) -> None:
        self._should_raise = error

    async def refresh_access_token(
        self,
        db: AsyncSession,
        integration_short_name: str,
        ctx: ApiContext,
        connection_id: UUID,
        decrypted_credential: dict,
        config_fields: Optional[dict] = None,
    ) -> FakeOAuth2TokenResponse:
        self._calls.append(
            (
                "refresh_access_token",
                db,
                integration_short_name,
                ctx,
                connection_id,
                decrypted_credential,
                config_fields,
            )
        )
        if self._should_raise:
            raise self._should_raise
        response = self._responses.get(integration_short_name)
        if not response:
            raise ValueError(f"No seeded response for {integration_short_name}")
        return response
