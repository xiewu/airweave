"""API endpoints for managing API keys."""

from uuid import UUID

from fastapi import Body, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api import deps
from airweave.api.router import TrailingSlashRouter
from airweave.core import credentials
from airweave.schemas.auth import AuthContext

router = TrailingSlashRouter()


@router.post("/", response_model=schemas.APIKey)
async def create_api_key(
    *,
    db: AsyncSession = Depends(deps.get_db),
    api_key_in: schemas.APIKeyCreate = Body({}),  # Default to empty dict if not provided
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.APIKey:
    """Create a new API key for the current user.

    Returns a temporary plain key for the user to store securely.
    This is not stored in the database.

    Args:
    ----
        db (AsyncSession): The database session.
        api_key_in (schemas.APIKeyCreate): The API key creation data.
        auth_context (AuthContext): The current authentication context.

    Returns:
    -------
        schemas.APIKey: The created API key object, including the key.

    """
    api_key_obj = await crud.api_key.create(db=db, obj_in=api_key_in, auth_context=auth_context)

    # Decrypt the key for the response
    decrypted_data = credentials.decrypt(api_key_obj.encrypted_key)
    decrypted_key = decrypted_data["key"]

    api_key_data = {
        "id": api_key_obj.id,
        "organization": auth_context.organization_id,  # Use the user's organization_id
        "created_at": api_key_obj.created_at,
        "modified_at": api_key_obj.modified_at,
        "last_used_date": None,  # New key has no last used date
        "expiration_date": api_key_obj.expiration_date,
        "created_by_email": api_key_obj.created_by_email,
        "modified_by_email": api_key_obj.modified_by_email,
        "decrypted_key": decrypted_key,
    }

    return schemas.APIKey(**api_key_data)


@router.get("/{id}", response_model=schemas.APIKey)
async def read_api_key(
    *,
    db: AsyncSession = Depends(deps.get_db),
    id: UUID,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.APIKey:
    """Retrieve an API key by ID.

    Args:
    ----
        db (AsyncSession): The database session.
        id (UUID): The ID of the API key.
        auth_context (AuthContext): The current authentication context.

    Returns:
    -------
        schemas.APIKey: The API key object with decrypted key.

    Raises:
    ------
        HTTPException: If the API key is not found.
    """
    api_key = await crud.api_key.get(db=db, id=id, auth_context=auth_context)
    # Decrypt the key for the response
    decrypted_data = credentials.decrypt(api_key.encrypted_key)
    decrypted_key = decrypted_data["key"]

    api_key_data = {
        "id": api_key.id,
        "organization": auth_context.organization_id,
        "created_at": api_key.created_at,
        "modified_at": api_key.modified_at,
        "last_used_date": api_key.last_used_date if hasattr(api_key, "last_used_date") else None,
        "expiration_date": api_key.expiration_date,
        "created_by_email": api_key.created_by_email,
        "modified_by_email": api_key.modified_by_email,
        "decrypted_key": decrypted_key,
    }

    return schemas.APIKey(**api_key_data)


@router.get("/", response_model=list[schemas.APIKey])
async def read_api_keys(
    *,
    db: AsyncSession = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> list[schemas.APIKey]:
    """Retrieve all API keys for the current user.

    Args:
    ----
        db (AsyncSession): The database session.
        skip (int): Number of records to skip for pagination.
        limit (int): Maximum number of records to return.
        auth_context (AuthContext): The current authentication context.

    Returns:
    -------
        List[schemas.APIKey]: A list of API keys with decrypted keys.
    """
    api_keys = await crud.api_key.get_multi(
        db=db, skip=skip, limit=limit, auth_context=auth_context
    )

    result = []
    for api_key in api_keys:
        # Decrypt each key
        decrypted_data = credentials.decrypt(api_key.encrypted_key)
        decrypted_key = decrypted_data["key"]

        api_key_data = {
            "id": api_key.id,
            "organization": auth_context.organization_id,
            "created_at": api_key.created_at,
            "modified_at": api_key.modified_at,
            "last_used_date": (
                api_key.last_used_date if hasattr(api_key, "last_used_date") else None
            ),
            "expiration_date": api_key.expiration_date,
            "created_by_email": api_key.created_by_email,
            "modified_by_email": api_key.modified_by_email,
            "decrypted_key": decrypted_key,
        }
        result.append(schemas.APIKey(**api_key_data))

    return result


@router.delete("/", response_model=schemas.APIKey)
async def delete_api_key(
    *,
    db: AsyncSession = Depends(deps.get_db),
    id: UUID,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.APIKey:
    """Delete an API key.

    Args:
    ----
        db (AsyncSession): The database session.
        id (UUID): The ID of the API key.
        auth_context (AuthContext): The current authentication context.

    Returns:
    -------
        schemas.APIKey: The revoked API key object.

    Raises:
    ------
        HTTPException: If the API key is not found.

    """
    api_key = await crud.api_key.get(db=db, id=id, auth_context=auth_context)

    # Decrypt the key for the response
    decrypted_data = credentials.decrypt(api_key.encrypted_key)
    decrypted_key = decrypted_data["key"]

    # Create a copy of the data before deletion
    api_key_data = {
        "id": api_key.id,
        "organization": auth_context.organization_id,
        "created_at": api_key.created_at,
        "modified_at": api_key.modified_at,
        "last_used_date": api_key.last_used_date if hasattr(api_key, "last_used_date") else None,
        "expiration_date": api_key.expiration_date,
        "created_by_email": api_key.created_by_email,
        "modified_by_email": api_key.modified_by_email,
        "decrypted_key": decrypted_key,
    }

    # Now delete the API key
    await crud.api_key.remove(db=db, id=id, auth_context=auth_context)

    return schemas.APIKey(**api_key_data)
