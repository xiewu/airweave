"""Service for data synchronization."""

from typing import List, Optional, Union
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.core.dag_service import dag_service
from airweave.core.datetime_utils import utc_now_naive
from airweave.core.logging import logger
from airweave.core.shared_models import SyncJobStatus
from airweave.core.sync_job_service import sync_job_service
from airweave.db.session import get_db_context
from airweave.db.unit_of_work import UnitOfWork
from airweave.platform.sync.factory import SyncFactory
from airweave.schemas.auth import AuthContext


class SyncService:
    """Main service for data synchronization."""

    async def create(
        self,
        db: AsyncSession,
        sync: schemas.SyncCreate,
        auth_context: AuthContext,
        uow: UnitOfWork,
    ) -> schemas.Sync:
        """Create a new sync.

        This function creates a new sync and then creates the initial DAG for it. It uses an
        externally scoped unit of work to ensure that the sync and DAG are created in a single
        transaction, with rollback on error.

        Args:
        ----
            db (AsyncSession): The database session.
            sync (schemas.SyncCreate): The sync to create.
            auth_context (AuthContext): The authentication context.
            uow (UnitOfWork): The unit of work.

        Returns:
        -------
            schemas.Sync: The created sync.
        """
        sync = await crud.sync.create(
            db=db,
            obj_in=sync,
            auth_context=auth_context,
            uow=uow,
        )
        await uow.session.flush()
        await dag_service.create_initial_dag(
            db=db, sync_id=sync.id, auth_context=auth_context, uow=uow
        )
        return sync

    async def run(
        self,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        dag: schemas.SyncDag,
        collection: schemas.Collection,
        source_connection: schemas.Connection,
        auth_context: AuthContext,
        access_token: Optional[str] = None,
    ) -> schemas.Sync:
        """Run a sync.

        Args:
        ----
            sync (schemas.Sync): The sync to run.
            sync_job (schemas.SyncJob): The sync job to run.
            dag (schemas.SyncDag): The DAG to run.
            collection (schemas.Collection): The collection to sync.
            source_connection (schemas.Connection): The source connection to sync.
            auth_context (AuthContext): The authentication context.
            access_token (Optional[str]): Optional access token to use
                instead of stored credentials.

        Returns:
        -------
            schemas.Sync: The sync.
        """
        try:
            async with get_db_context() as db:
                # Create dedicated orchestrator instance
                orchestrator = await SyncFactory.create_orchestrator(
                    db=db,
                    sync=sync,
                    sync_job=sync_job,
                    dag=dag,
                    collection=collection,
                    source_connection=source_connection,
                    auth_context=auth_context,
                    access_token=access_token,
                )
        except Exception as e:
            logger.error(f"Error during sync orchestrator creation: {e}")
            # Fail the sync job if orchestrator creation failed
            await sync_job_service.update_status(
                sync_job_id=sync_job.id,
                status=SyncJobStatus.FAILED,
                auth_context=auth_context,
                error=str(e),
                failed_at=utc_now_naive(),
            )
            raise e

        # Run the sync with the dedicated orchestrator instance
        return await orchestrator.run()

    async def list_syncs(
        self,
        db: AsyncSession,
        auth_context: AuthContext,
        skip: int = 0,
        limit: int = 100,
        with_source_connection: bool = False,
    ) -> Union[List[schemas.Sync], List[schemas.SyncWithSourceConnection]]:
        """List all syncs for a user.

        Args:
        ----
            db (AsyncSession): The database session.
            auth_context (AuthContext): The authentication context.
            skip (int): The number of syncs to skip.
            limit (int): The number of syncs to return.
            with_source_connection (bool): Whether to include source connections.

        Returns:
        -------
            Union[List[schemas.Sync], List[schemas.SyncWithSourceConnection]]: A list of syncs.
        """
        if with_source_connection:
            syncs = await crud.sync.get_all_syncs_join_with_source_connection(
                db=db, auth_context=auth_context
            )
        else:
            syncs = await crud.sync.get_multi(
                db=db, auth_context=auth_context, skip=skip, limit=limit
            )
        return syncs

    async def get_sync(
        self,
        db: AsyncSession,
        sync_id: UUID,
        auth_context: AuthContext,
        with_connections: bool = False,
    ) -> schemas.Sync:
        """Get a specific sync by ID.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_id (UUID): The ID of the sync to get.
            auth_context (AuthContext): The authentication context.
            with_connections (bool): Whether to include connections.

        Returns:
        -------
            schemas.Sync: The sync.

        Raises:
        ------
            HTTPException: If the sync is not found.
        """
        sync = await crud.sync.get(
            db=db, id=sync_id, auth_context=auth_context, with_connections=with_connections
        )
        if not sync:
            raise HTTPException(status_code=404, detail="Sync not found")
        return sync

    async def delete_sync(
        self,
        db: AsyncSession,
        sync_id: UUID,
        auth_context: AuthContext,
        delete_data: bool = False,
        uow: Optional[UnitOfWork] = None,
    ) -> schemas.Sync:
        """Delete a sync configuration and optionally its associated data.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_id (UUID): The ID of the sync to delete.
            auth_context (AuthContext): The authentication context.
            delete_data (bool): Whether to delete the data.
            uow (Optional[UnitOfWork]): The unit of work.

        Returns:
        -------
            schemas.Sync: The deleted sync.

        Raises:
        ------
            HTTPException: If the sync is not found.
        """
        sync = await crud.sync.get(db=db, id=sync_id, auth_context=auth_context)
        if not sync:
            raise HTTPException(status_code=404, detail="Sync not found")

        if delete_data:
            # TODO: Implement data deletion logic, should be part of destination interface
            pass

        return await crud.sync.remove(db=db, id=sync_id, auth_context=auth_context, uow=uow)

    async def _create_and_run_sync_internal(
        self,
        sync_in: schemas.SyncCreate,
        auth_context: AuthContext,
        uow: UnitOfWork,
    ) -> tuple[schemas.Sync, Optional[schemas.SyncJob]]:
        """Internal helper method for creating and running a sync.

        Args:
        ----
            sync_in (schemas.SyncCreate): The sync to create.
            auth_context (AuthContext): The authentication context.
            uow (UnitOfWork): The unit of work to use.

        Returns:
        -------
            tuple[schemas.Sync, Optional[schemas.SyncJob]]: The created sync and job if run.
        """
        sync = await self.create(
            db=uow.session, sync=sync_in.to_base(), auth_context=auth_context, uow=uow
        )
        await uow.session.flush()
        sync_schema = schemas.Sync.model_validate(sync)

        sync_job = None
        if sync_in.run_immediately:
            sync_job_create = schemas.SyncJobCreate(sync_id=sync_schema.id)
            sync_job = await crud.sync_job.create(
                db=uow.session, obj_in=sync_job_create, auth_context=auth_context, uow=uow
            )
            await uow.session.flush()
            await uow.session.refresh(sync_job)
            sync_job = schemas.SyncJob.model_validate(sync_job)

        return sync_schema, sync_job

    async def create_and_run_sync(
        self,
        db: AsyncSession,
        sync_in: schemas.SyncCreate,
        auth_context: AuthContext,
        uow: Optional[UnitOfWork] = None,
    ) -> tuple[schemas.Sync, Optional[schemas.SyncJob]]:
        """Create a new sync and optionally run it immediately.

        TODO: Make it run immediately if sync_in.run_immediately is True
            Currently, it expects the FastAPI endpoint to trigger the run
            in background. This should be replaced by decentralized orchestration.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_in (schemas.SyncCreate): The sync to create.
            auth_context (AuthContext): The authentication context.
            uow (Optional[UnitOfWork]): Existing unit of work if provided, otherwise create new one.

        Returns:
        -------
            tuple[schemas.Sync, Optional[schemas.SyncJob]]: The created sync and job if run.
        """
        if uow is not None:
            # Use the provided UnitOfWork without managing its lifecycle
            return await self._create_and_run_sync_internal(sync_in, auth_context, uow)
        else:
            # Create and manage our own UnitOfWork
            async with UnitOfWork(db) as local_uow:
                result = await self._create_and_run_sync_internal(sync_in, auth_context, local_uow)
                await local_uow.commit()
                return result

    async def trigger_sync_run(
        self,
        db: AsyncSession,
        sync_id: UUID,
        auth_context: AuthContext,
    ) -> tuple[schemas.Sync, schemas.SyncJob, schemas.SyncDag]:
        """Trigger a sync run.

        TODO: Does not actually run the sync, just creates the job and DAG.
        The actual sync run is triggered by background task in the endpoint.
        At some point this method will distribute to task queue.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_id (UUID): The ID of the sync to run.
            auth_context (AuthContext): The authentication context.

        Returns:
        -------
            tuple[schemas.Sync, schemas.SyncJob, schemas.SyncDag]: The sync, job, and DAG.

        Raises:
        ------
            HTTPException: If the sync is not found.
        """
        sync = await crud.sync.get(
            db=db, id=sync_id, auth_context=auth_context, with_connections=True
        )
        if not sync:
            raise HTTPException(status_code=404, detail="Sync not found")

        sync_schema = schemas.Sync.model_validate(sync)

        sync_job_in = schemas.SyncJobCreate(sync_id=sync_id)
        sync_job = await crud.sync_job.create(db=db, obj_in=sync_job_in, auth_context=auth_context)
        await db.flush()
        sync_job_schema = schemas.SyncJob.model_validate(sync_job)

        sync_dag = await crud.sync_dag.get_by_sync_id(
            db=db, sync_id=sync_id, auth_context=auth_context
        )
        sync_dag_schema = schemas.SyncDag.model_validate(sync_dag)

        return sync_schema, sync_job_schema, sync_dag_schema

    async def list_sync_jobs(
        self,
        db: AsyncSession,
        auth_context: AuthContext,
        sync_id: Optional[UUID] = None,
        skip: int = 0,
        limit: int = 100,
        status: Optional[List[str]] = None,
    ) -> List[schemas.SyncJob]:
        """List sync jobs, either for all syncs or a specific sync.

        Args:
        ----
            db (AsyncSession): The database session.
            auth_context (AuthContext): The authentication context.
            sync_id (Optional[UUID]): The specific sync ID, if any.
            skip (int): The number of jobs to skip.
            limit (int): The number of jobs to return.
            status (Optional[List[str]]): Filter by job status.

        Returns:
        -------
            List[schemas.SyncJob]: A list of sync jobs.

        Raises:
        ------
            HTTPException: If the sync is not found (when sync_id is provided).
        """
        if sync_id:
            sync = await crud.sync.get(db=db, id=sync_id, auth_context=auth_context)
            if not sync:
                raise HTTPException(status_code=404, detail="Sync not found")
            return await crud.sync_job.get_all_by_sync_id(db=db, sync_id=sync_id)
        else:
            return await crud.sync_job.get_all_jobs(
                db=db, skip=skip, limit=limit, auth_context=auth_context, status=status
            )

    async def get_sync_job(
        self,
        db: AsyncSession,
        job_id: UUID,
        auth_context: AuthContext,
        sync_id: Optional[UUID] = None,
    ) -> schemas.SyncJob:
        """Get a specific sync job.

        Args:
        ----
            db (AsyncSession): The database session.
            job_id (UUID): The ID of the job to get.
            auth_context (AuthContext): The authentication context.
            sync_id (Optional[UUID]): The sync ID for validation.

        Returns:
        -------
            schemas.SyncJob: The sync job.

        Raises:
        ------
            HTTPException: If the job is not found or doesn't match the sync.
        """
        sync_job = await crud.sync_job.get(db=db, id=job_id, auth_context=auth_context)
        if not sync_job or (sync_id and sync_job.sync_id != sync_id):
            raise HTTPException(status_code=404, detail="Sync job not found")
        return sync_job

    async def get_sync_dag(
        self,
        db: AsyncSession,
        sync_id: UUID,
        auth_context: AuthContext,
    ) -> schemas.SyncDag:
        """Get the DAG for a specific sync.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_id (UUID): The ID of the sync.
            auth_context (AuthContext): The authentication context.

        Returns:
        -------
            schemas.SyncDag: The sync DAG.

        Raises:
        ------
            HTTPException: If the DAG is not found.
        """
        dag = await crud.sync_dag.get_by_sync_id(db=db, sync_id=sync_id, auth_context=auth_context)
        if not dag:
            raise HTTPException(status_code=404, detail=f"DAG for sync {sync_id} not found")
        return dag

    async def update_sync(
        self,
        db: AsyncSession,
        sync_id: UUID,
        sync_update: schemas.SyncUpdate,
        auth_context: AuthContext,
    ) -> schemas.Sync:
        """Update a sync configuration.

        Args:
        ----
            db (AsyncSession): The database session.
            sync_id (UUID): The ID of the sync to update.
            sync_update (schemas.SyncUpdate): The sync update data.
            auth_context (AuthContext): The authentication context.

        Returns:
        -------
            schemas.Sync: The updated sync.

        Raises:
        ------
            HTTPException: If the sync is not found.
        """
        sync = await crud.sync.get(
            db=db, id=sync_id, auth_context=auth_context, with_connections=False
        )
        if not sync:
            raise HTTPException(status_code=404, detail="Sync not found")

        await crud.sync.update(db=db, db_obj=sync, obj_in=sync_update, auth_context=auth_context)
        updated_sync = await crud.sync.get(
            db=db, id=sync_id, auth_context=auth_context, with_connections=True
        )
        return updated_sync


sync_service = SyncService()
