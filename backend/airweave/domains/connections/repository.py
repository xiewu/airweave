"""Connection repository wrapping crud.connection."""

from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.api.context import ApiContext
from airweave.domains.connections.protocols import ConnectionRepositoryProtocol
from airweave.models.connection import Connection


class ConnectionRepository(ConnectionRepositoryProtocol):
    """Delegates to the crud.connection singleton."""

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[Connection]:
        return await crud.connection.get(db, id, ctx)

    async def get_by_readable_id(
        self, db: AsyncSession, readable_id: str, ctx: ApiContext
    ) -> Optional[Connection]:
        return await crud.connection.get_by_readable_id(db, readable_id=readable_id, ctx=ctx)
