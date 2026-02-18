"""Source connection repository wrapping crud.source_connection."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.api.context import ApiContext
from airweave.domains.source_connections.protocols import (
    SourceConnectionRepositoryProtocol,
)
from airweave.models.source_connection import SourceConnection


class SourceConnectionRepository(SourceConnectionRepositoryProtocol):
    """Delegates to the crud.source_connection singleton."""

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        return await crud.source_connection.get(db, id, ctx)
