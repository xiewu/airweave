"""Legacy source connection service with only active entry points."""

from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api.context import ApiContext
from airweave.core.shared_models import SyncJobStatus
from airweave.core.temporal_service import temporal_service
from airweave.crud import connection_init_session
from airweave.models.connection_init_session import ConnectionInitStatus


class SourceConnectionService:
    """Service for source-connection callback and cancellation lifecycle."""

    async def cancel_job(
        self,
        db: AsyncSession,
        *,
        source_connection_id: UUID,
        job_id: UUID,
        ctx: ApiContext,
    ) -> schemas.SourceConnectionJob:
        """Cancel a running sync job for a source connection."""
        source_conn = await crud.source_connection.get(db, id=source_connection_id, ctx=ctx)
        if not source_conn:
            raise HTTPException(status_code=404, detail="Source connection not found")

        if not source_conn.sync_id:
            raise HTTPException(status_code=400, detail="Source connection has no associated sync")

        sync_job = await crud.sync_job.get(db, id=job_id, ctx=ctx)
        if not sync_job:
            raise HTTPException(status_code=404, detail="Sync job not found")

        if sync_job.sync_id != source_conn.sync_id:
            raise HTTPException(
                status_code=400, detail="Sync job does not belong to this source connection"
            )

        if sync_job.status not in [SyncJobStatus.PENDING, SyncJobStatus.RUNNING]:
            raise HTTPException(
                status_code=400, detail=f"Cannot cancel job in {sync_job.status} state"
            )

        from airweave.core.sync_job_service import sync_job_service

        await sync_job_service.update_status(
            sync_job_id=job_id,
            status=SyncJobStatus.CANCELLING,
            ctx=ctx,
        )

        cancel_result = await temporal_service.cancel_sync_job_workflow(str(job_id), ctx)

        if not cancel_result["success"]:
            fallback_status = (
                SyncJobStatus.RUNNING
                if sync_job.status == SyncJobStatus.RUNNING
                else SyncJobStatus.PENDING
            )
            await sync_job_service.update_status(
                sync_job_id=job_id,
                status=fallback_status,
                ctx=ctx,
            )
            raise HTTPException(
                status_code=502, detail="Failed to request cancellation from Temporal"
            )

        if not cancel_result["workflow_found"]:
            ctx.logger.info(f"Workflow not found for job {job_id} - marking as CANCELLED directly")
            from airweave.core.datetime_utils import utc_now_naive

            await sync_job_service.update_status(
                sync_job_id=job_id,
                status=SyncJobStatus.CANCELLED,
                ctx=ctx,
                completed_at=utc_now_naive(),
                error="Workflow not found in Temporal - may have already completed",
            )

        await db.refresh(sync_job)
        sync_job_schema = schemas.SyncJob.model_validate(sync_job, from_attributes=True)
        return sync_job_schema.to_source_connection_job(source_connection_id)

    async def complete_oauth1_callback(
        self,
        db: AsyncSession,
        *,
        oauth_token: str,
        oauth_verifier: str,
    ) -> schemas.SourceConnection:
        """Complete OAuth1 flow from callback."""
        init_session = await connection_init_session.get_by_oauth_token_no_auth(
            db, oauth_token=oauth_token
        )
        if not init_session:
            raise HTTPException(
                status_code=404,
                detail=(
                    "OAuth1 session not found or expired. Request token may have been used already."
                ),
            )

        if init_session.status != ConnectionInitStatus.PENDING:
            raise HTTPException(
                status_code=400, detail=f"OAuth session already {init_session.status}"
            )

        ctx = await self._reconstruct_context_from_session(db, init_session)

        source_conn_shell = await crud.source_connection.get_by_query_and_org(
            db, ctx=ctx, connection_init_session_id=init_session.id
        )
        if not source_conn_shell:
            raise HTTPException(status_code=404, detail="Source connection shell not found")

        from airweave.platform.auth.schemas import OAuth1Settings
        from airweave.platform.auth.settings import integration_settings

        oauth_settings = await integration_settings.get_by_short_name(init_session.short_name)

        if not isinstance(oauth_settings, OAuth1Settings):
            raise HTTPException(
                status_code=400,
                detail=f"Source {init_session.short_name} is not configured for OAuth1",
            )

        token_response = await self._exchange_oauth1_code(
            init_session.short_name,
            oauth_verifier,
            init_session.overrides,
            oauth_settings,
            ctx,
        )
        source_conn = await self._complete_oauth1_connection(
            db, source_conn_shell, init_session, token_response, ctx
        )
        return await self._finalize_oauth_callback(db, source_conn, ctx)

    async def complete_oauth2_callback(
        self,
        db: AsyncSession,
        *,
        state: str,
        code: str,
    ) -> schemas.SourceConnection:
        """Complete OAuth2 flow from callback."""
        init_session = await connection_init_session.get_by_state_no_auth(db, state=state)
        if not init_session:
            raise HTTPException(status_code=404, detail="OAuth2 session not found or expired")

        if init_session.status != ConnectionInitStatus.PENDING:
            raise HTTPException(
                status_code=400, detail=f"OAuth session already {init_session.status}"
            )

        ctx = await self._reconstruct_context_from_session(db, init_session)

        source_conn_shell = await crud.source_connection.get_by_query_and_org(
            db, ctx=ctx, connection_init_session_id=init_session.id
        )
        if not source_conn_shell:
            raise HTTPException(status_code=404, detail="Source connection shell not found")

        token_response = await self._exchange_oauth2_code(
            init_session.short_name, code, init_session.overrides, ctx
        )

        await self._validate_oauth_token(
            db,
            await crud.source.get_by_short_name(db, short_name=init_session.short_name),
            token_response.access_token,
            None,
            ctx,
        )

        source_conn = await self._complete_oauth2_connection(
            db, source_conn_shell, init_session, token_response, ctx
        )

        return await self._finalize_oauth_callback(db, source_conn, ctx)

    async def _finalize_oauth_callback(
        self,
        db: AsyncSession,
        source_conn: Any,
        ctx,
    ) -> schemas.SourceConnection:
        """Build response and trigger run-immediately workflow for OAuth callbacks."""
        source_conn_response = await self._build_source_connection_response(db, source_conn, ctx)

        if source_conn.sync_id:
            sync = await crud.sync.get(db, id=source_conn.sync_id, ctx=ctx)
            if sync:
                jobs = await crud.sync_job.get_all_by_sync_id(db, sync_id=sync.id)
                if jobs and len(jobs) > 0:
                    sync_job = jobs[0]
                    if sync_job.status == SyncJobStatus.PENDING:
                        collection = await crud.collection.get_by_readable_id(
                            db, readable_id=source_conn.readable_collection_id, ctx=ctx
                        )
                        if collection:
                            collection_schema = schemas.Collection.model_validate(
                                collection, from_attributes=True
                            )
                            sync_job_schema = schemas.SyncJob.model_validate(
                                sync_job, from_attributes=True
                            )
                            sync_schema = schemas.Sync.model_validate(sync, from_attributes=True)
                            connection_schema = await self._get_connection_for_source_connection(
                                db=db, source_connection=source_conn, ctx=ctx
                            )

                            await temporal_service.run_source_connection_workflow(
                                sync=sync_schema,
                                sync_job=sync_job_schema,
                                collection=collection_schema,
                                connection=connection_schema,
                                ctx=ctx,
                            )

        return source_conn_response

    from airweave.core.source_connection_service_helpers import (
        source_connection_helpers,
    )

    _validate_oauth_token = source_connection_helpers.validate_oauth_token
    _build_source_connection_response = source_connection_helpers.build_source_connection_response
    _reconstruct_context_from_session = source_connection_helpers.reconstruct_context_from_session
    _exchange_oauth1_code = source_connection_helpers.exchange_oauth1_code
    _exchange_oauth2_code = source_connection_helpers.exchange_oauth2_code
    _complete_oauth1_connection = source_connection_helpers.complete_oauth1_connection
    _complete_oauth2_connection = source_connection_helpers.complete_oauth2_connection
    _get_connection_for_source_connection = (
        source_connection_helpers.get_connection_for_source_connection
    )


source_connection_service = SourceConnectionService()
