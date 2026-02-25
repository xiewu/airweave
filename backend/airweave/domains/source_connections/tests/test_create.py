"""Unit tests for SourceConnectionCreationService."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.context import ApiContext
from airweave.core.exceptions import NotFoundException
from airweave.core.shared_models import AuthMethod
from airweave.domains.collections.fakes.repository import FakeCollectionRepository
from airweave.domains.connections.fakes.repository import FakeConnectionRepository
from airweave.domains.credentials.fakes.repository import FakeIntegrationCredentialRepository
from airweave.domains.source_connections.create import SourceConnectionCreationService
from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository
from airweave.domains.source_connections.fakes.response import FakeResponseBuilder
from airweave.domains.source_connections.fakes.service import FakeSourceConnectionService
from airweave.domains.sources.exceptions import SourceValidationError
from airweave.domains.sources.fakes.lifecycle import FakeSourceLifecycleService
from airweave.domains.sources.fakes.registry import FakeSourceRegistry
from airweave.domains.sources.fakes.validation import FakeSourceValidationService
from airweave.domains.syncs.fakes.sync_lifecycle_service import FakeSyncLifecycleService
from airweave.domains.syncs.fakes.sync_record_service import FakeSyncRecordService
from airweave.domains.temporal.fakes.service import FakeTemporalWorkflowService
from airweave.schemas.organization import Organization
from airweave.schemas.source_connection import (
    AuthenticationMethod,
    AuthProviderAuthentication,
    DirectAuthentication,
    OAuthBrowserAuthentication,
    OAuthTokenAuthentication,
    SourceConnectionCreate,
)


NOW = datetime.now(timezone.utc)


def _ctx() -> ApiContext:
    org = Organization(id=str(uuid4()), name="Test Org", created_at=NOW, modified_at=NOW)
    return ApiContext(
        request_id="test-req",
        organization=org,
        auth_method=AuthMethod.SYSTEM,
    )


def _entry(*, oauth_type=None, requires_byoc=False, supports_continuous=False):
    source_cls = MagicMock()
    source_cls.requires_byoc = requires_byoc
    source_cls.supports_auth_method.return_value = True
    source_cls.get_supported_auth_methods.return_value = [
        AuthenticationMethod.DIRECT,
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
        AuthenticationMethod.OAUTH_BYOC,
    ]
    return SimpleNamespace(
        name="GitHub",
        short_name="github",
        source_class_ref=source_cls,
        oauth_type=oauth_type,
        supports_continuous=supports_continuous,
        federated_search=False,
        config_ref=None,
        auth_config_ref=None,
        supported_auth_providers=["pipedream", "composio"],
    )


def _service(entry) -> SourceConnectionCreationService:
    registry = FakeSourceRegistry()
    registry.get = MagicMock(return_value=entry)
    return SourceConnectionCreationService(
        sc_repo=FakeSourceConnectionRepository(),
        collection_repo=FakeCollectionRepository(),
        connection_repo=FakeConnectionRepository(),
        credential_repo=FakeIntegrationCredentialRepository(),
        source_registry=registry,
        source_validation=FakeSourceValidationService(),
        source_lifecycle=FakeSourceLifecycleService(),
        sync_lifecycle=FakeSyncLifecycleService(),
        sync_record_service=FakeSyncRecordService(),
        response_builder=FakeResponseBuilder(),
        oauth1_service=AsyncMock(),
        oauth2_service=AsyncMock(),
        credential_encryptor=MagicMock(),
        temporal_workflow_service=FakeTemporalWorkflowService(),
        event_bus=AsyncMock(),
    )


async def test_create_dispatches_direct_branch():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=True),
    )
    svc._create_with_direct_auth = AsyncMock(return_value=expected)
    svc._create_with_oauth_token = AsyncMock()
    svc._create_with_auth_provider = AsyncMock()
    svc._create_with_oauth_browser = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"token": "abc"}),
    )
    result = await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert result is expected
    svc._create_with_direct_auth.assert_awaited_once()


async def test_create_dispatches_oauth_token_branch():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=True),
    )
    svc._create_with_oauth_token = AsyncMock(return_value=expected)
    svc._create_with_direct_auth = AsyncMock()
    svc._create_with_auth_provider = AsyncMock()
    svc._create_with_oauth_browser = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthTokenAuthentication(access_token="tok"),
    )
    result = await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert result is expected
    svc._create_with_oauth_token.assert_awaited_once()


async def test_create_dispatches_auth_provider_branch():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=True),
    )
    svc._create_with_auth_provider = AsyncMock(return_value=expected)
    svc._create_with_direct_auth = AsyncMock()
    svc._create_with_oauth_token = AsyncMock()
    svc._create_with_oauth_browser = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=AuthProviderAuthentication(provider_readable_id="provider-1"),
    )
    result = await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert result is expected
    svc._create_with_auth_provider.assert_awaited_once()


async def test_create_dispatches_oauth_browser_branch_and_defaults_sync_false():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=False),
    )
    svc._create_with_oauth_browser = AsyncMock(return_value=expected)
    svc._create_with_direct_auth = AsyncMock()
    svc._create_with_oauth_token = AsyncMock()
    svc._create_with_auth_provider = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(),
    )
    result = await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert result is expected
    assert obj_in.sync_immediately is False
    svc._create_with_oauth_browser.assert_awaited_once()


async def test_create_defaults_sync_true_for_direct_when_unset():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=True),
    )
    svc._create_with_direct_auth = AsyncMock(return_value=expected)

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"token": "abc"}),
    )
    await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert obj_in.sync_immediately is True


async def test_create_raises_when_source_connection_created_event_fails():
    svc = _service(_entry())
    expected = MagicMock(
        id=uuid4(),
        short_name="github",
        readable_collection_id="col-1",
        auth=SimpleNamespace(authenticated=True),
    )
    svc._create_with_direct_auth = AsyncMock(return_value=expected)
    svc._event_bus.publish = AsyncMock(side_effect=RuntimeError("bus down"))
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"token": "abc"}),
    )
    with pytest.raises(RuntimeError, match="bus down"):
        await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())


async def test_create_rejects_browser_sync_immediately_true():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        sync_immediately=True,
        authentication=OAuthBrowserAuthentication(),
    )
    with pytest.raises(HTTPException, match="cannot use sync_immediately"):
        await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())


async def test_create_rejects_missing_byoc_for_required_source():
    svc = _service(_entry(requires_byoc=True))
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(),
    )
    with pytest.raises(HTTPException, match="requires custom OAuth client credentials"):
        await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())


async def test_create_oauth2_init_session_contract(monkeypatch):
    entry = _entry(oauth_type="access_only")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={"instance_url": "acme"})
    svc._extract_template_configs = MagicMock(return_value={"instance_url": "acme"})
    svc._collection_repo.seed_readable("col-1", MagicMock(readable_id="col-1"))
    svc._oauth2_service.generate_auth_url_with_redirect = AsyncMock(
        return_value=("https://provider/auth", "verifier-123")
    )
    from airweave.platform.auth.schemas import OAuth2Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth2Settings(
                integration_short_name="github",
                url="https://provider/authorize",
                backend_url="https://provider/token",
                grant_type="authorization_code",
                client_id="platform-client-id",
                client_secret="platform-client-secret",
                content_type="application/x-www-form-urlencoded",
                client_credential_location="payload",
            )
        ),
    )

    shell_sc = MagicMock(id=uuid4(), connection_init_session_id=None, is_authenticated=False)
    svc._sc_repo.create = AsyncMock(return_value=shell_sc)
    svc._response_builder.build_response = AsyncMock(return_value=MagicMock(id=shell_sc.id))

    from airweave.domains.source_connections import create as create_module

    captured = {}

    async def _fake_create_redirect_session(db, provider_auth_url, ctx, uow):
        return uuid4()

    async def _fake_create_init_session(db, obj_in, state, ctx, uow, **kwargs):
        captured.update(kwargs)
        return MagicMock(id=uuid4())

    monkeypatch.setattr(svc, "_create_redirect_session", _fake_create_redirect_session)
    monkeypatch.setattr(svc, "_create_init_session", _fake_create_init_session)

    class _FakeUOW:
        def __init__(self, db):
            self.session = db

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(create_module, "UnitOfWork", _FakeUOW)

    db = AsyncMock()
    db.add = MagicMock()
    db.refresh = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(),
    )
    await svc._create_with_oauth_browser(db, obj_in=obj_in, entry=entry, ctx=_ctx())

    assert captured["template_configs"] == {"instance_url": "acme"}
    assert captured["additional_overrides"]["code_verifier"] == "verifier-123"
    assert "redirect_session_id" in captured


async def test_create_oauth1_init_session_contract(monkeypatch):
    entry = _entry(oauth_type="oauth1")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    svc._collection_repo.seed_readable("col-1", MagicMock(readable_id="col-1"))
    svc._oauth1_service.get_request_token = AsyncMock(
        return_value=SimpleNamespace(oauth_token="req-token", oauth_token_secret="req-secret")
    )
    svc._oauth1_service.build_authorization_url = MagicMock(return_value="https://provider/oauth1-auth")

    shell_sc = MagicMock(id=uuid4(), connection_init_session_id=None, is_authenticated=False)
    svc._sc_repo.create = AsyncMock(return_value=shell_sc)
    svc._response_builder.build_response = AsyncMock(return_value=MagicMock(id=shell_sc.id))

    from airweave.platform.auth.schemas import OAuth1Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth1Settings(
                integration_short_name="github",
                request_token_url="https://provider/request-token",
                authorization_url="https://provider/authorize",
                access_token_url="https://provider/access-token",
                consumer_key="platform-key",
                consumer_secret="platform-secret",
            )
        ),
    )

    from airweave.domains.source_connections import create as create_module

    captured = {}

    async def _fake_create_redirect_session(db, provider_auth_url, ctx, uow):
        return uuid4()

    async def _fake_create_init_session(db, obj_in, state, ctx, uow, **kwargs):
        captured.update(kwargs)
        return MagicMock(id=uuid4())

    monkeypatch.setattr(svc, "_create_redirect_session", _fake_create_redirect_session)
    monkeypatch.setattr(svc, "_create_init_session", _fake_create_init_session)

    class _FakeUOW:
        def __init__(self, db):
            self.session = db

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(create_module, "UnitOfWork", _FakeUOW)

    db = AsyncMock()
    db.add = MagicMock()
    db.refresh = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(
            consumer_key="custom-key",
            consumer_secret="custom-secret",
        ),
    )
    await svc._create_with_oauth_browser(db, obj_in=obj_in, entry=entry, ctx=_ctx())

    overrides = captured["additional_overrides"]
    assert overrides["oauth_token"] == "req-token"
    assert overrides["oauth_token_secret"] == "req-secret"
    assert overrides["consumer_key"] == "custom-key"
    assert overrides["consumer_secret"] == "custom-secret"


def test_determine_auth_method_rejects_unknown_auth_shape():
    obj_in = SimpleNamespace(authentication=object())
    with pytest.raises(HTTPException, match="Invalid authentication configuration"):
        SourceConnectionCreationService._determine_auth_method(obj_in)  # type: ignore[arg-type]


def test_determine_auth_method_variants():
    assert (
        SourceConnectionCreationService._determine_auth_method(SimpleNamespace(authentication=None))
        == AuthenticationMethod.OAUTH_BROWSER
    )
    assert (
        SourceConnectionCreationService._determine_auth_method(
            SimpleNamespace(authentication=DirectAuthentication(credentials={"k": "v"}))
        )
        == AuthenticationMethod.DIRECT
    )
    assert (
        SourceConnectionCreationService._determine_auth_method(
            SimpleNamespace(authentication=OAuthTokenAuthentication(access_token="tok"))
        )
        == AuthenticationMethod.OAUTH_TOKEN
    )
    assert (
        SourceConnectionCreationService._determine_auth_method(
            SimpleNamespace(authentication=AuthProviderAuthentication(provider_readable_id="p1"))
        )
        == AuthenticationMethod.AUTH_PROVIDER
    )
    assert (
        SourceConnectionCreationService._determine_auth_method(
            SimpleNamespace(
                authentication=OAuthBrowserAuthentication(client_id="c", client_secret="s")
            )
        )
        == AuthenticationMethod.OAUTH_BYOC
    )


async def test_trigger_sync_workflow_logs_warning_when_event_publish_fails():
    svc = _service(_entry())
    svc._event_bus.publish = AsyncMock(side_effect=RuntimeError("bus down"))
    svc._temporal_workflow_service.run_source_connection_workflow = AsyncMock()

    ctx = _ctx()
    ctx.logger.warning = MagicMock()

    sync_job_id = uuid4()
    sync_id = uuid4()
    sync_result = SimpleNamespace(
        sync_job=SimpleNamespace(id=sync_job_id),
        sync_id=sync_id,
        sync=SimpleNamespace(id=sync_id),
    )
    connection = SimpleNamespace(short_name="github")
    collection = SimpleNamespace(id=uuid4(), name="c", readable_id="col-1")

    await svc._trigger_sync_workflow(
        connection=connection,
        sync_result=sync_result,
        collection=collection,
        source_connection_id=uuid4(),
        ctx=ctx,
    )

    ctx.logger.warning.assert_called_once()
    svc._temporal_workflow_service.run_source_connection_workflow.assert_awaited_once()


async def test_trigger_sync_workflow_returns_early_without_sync_job():
    svc = _service(_entry())
    svc._event_bus.publish = AsyncMock()
    svc._temporal_workflow_service.run_source_connection_workflow = AsyncMock()
    sync_result = SimpleNamespace(sync_job=None, sync_id=uuid4(), sync=SimpleNamespace(id=uuid4()))

    await svc._trigger_sync_workflow(
        connection=SimpleNamespace(short_name="github"),
        sync_result=sync_result,
        collection=SimpleNamespace(id=uuid4(), name="c", readable_id="col-1"),
        source_connection_id=uuid4(),
        ctx=_ctx(),
    )

    svc._event_bus.publish.assert_not_called()
    svc._temporal_workflow_service.run_source_connection_workflow.assert_not_called()


def test_extract_template_configs_maps_validation_error_to_http_422():
    class _Config:
        @staticmethod
        def get_template_config_fields():
            return ["instance_url"]

        @staticmethod
        def validate_template_configs(_validated_config):
            raise ValueError("Missing required template config: instance_url")

        @staticmethod
        def extract_template_configs(_validated_config):
            return {"instance_url": "acme"}

    entry = SimpleNamespace(config_ref=_Config)

    with pytest.raises(HTTPException) as exc_info:
        SourceConnectionCreationService._extract_template_configs(entry, {})

    assert exc_info.value.status_code == 422
    assert "Missing required template config" in str(exc_info.value.detail)


def test_extract_template_configs_returns_none_for_missing_config_ref():
    entry = SimpleNamespace(config_ref=None)
    assert SourceConnectionCreationService._extract_template_configs(entry, {"a": 1}) is None


def test_extract_template_configs_returns_none_without_template_method():
    class _Config:
        pass

    entry = SimpleNamespace(config_ref=_Config)
    assert SourceConnectionCreationService._extract_template_configs(entry, {"a": 1}) is None


def test_extract_template_configs_returns_none_when_template_fields_empty():
    class _Config:
        @staticmethod
        def get_template_config_fields():
            return []

    entry = SimpleNamespace(config_ref=_Config)
    assert SourceConnectionCreationService._extract_template_configs(entry, {"a": 1}) is None


def test_validate_auth_compatibility_raises_with_supported_methods():
    source_class = MagicMock()
    source_class.supports_auth_method.return_value = False
    source_class.get_supported_auth_methods.return_value = [
        AuthenticationMethod.DIRECT,
        AuthenticationMethod.OAUTH_TOKEN,
    ]

    with pytest.raises(HTTPException) as exc_info:
        SourceConnectionCreationService._validate_auth_compatibility(
            source_class, "github", AuthenticationMethod.OAUTH_BROWSER
        )

    assert exc_info.value.status_code == 400
    assert "Supported methods" in exc_info.value.detail


def test_get_source_entry_raises_404_when_missing():
    svc = _service(_entry())
    svc._source_registry.get = MagicMock(side_effect=KeyError("missing"))
    with pytest.raises(HTTPException) as exc_info:
        svc._get_source_entry("nope")
    assert exc_info.value.status_code == 404


async def test_get_collection_raises_not_found():
    svc = _service(_entry())
    with pytest.raises(NotFoundException):
        await svc._get_collection(AsyncMock(), "missing-col", _ctx())


async def test_create_with_oauth_token_requires_token_auth():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException, match="requires token"):
        await svc._create_with_oauth_token(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_oauth_token_builds_full_payload_and_delegates():
    svc = _service(_entry())
    svc._source_validation.validate_config = MagicMock(return_value={"cfg": "x"})
    svc._source_lifecycle.validate = AsyncMock()
    svc._create_authenticated_connection = AsyncMock(return_value=MagicMock(id=uuid4()))

    expires = datetime.now(timezone.utc) + timedelta(minutes=5)
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthTokenAuthentication(
            access_token="tok",
            refresh_token="rtok",
            expires_at=expires,
        ),
    )
    await svc._create_with_oauth_token(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())

    kwargs = svc._create_authenticated_connection.await_args.kwargs
    assert kwargs["credential_payload"]["access_token"] == "tok"
    assert kwargs["credential_payload"]["refresh_token"] == "rtok"
    assert kwargs["credential_payload"]["expires_at"] == expires.isoformat()
    assert kwargs["auth_method"] == AuthenticationMethod.OAUTH_TOKEN


async def test_create_with_oauth_token_maps_source_validation_error_to_400():
    svc = _service(_entry())
    svc._source_validation.validate_config = MagicMock(return_value={"cfg": "x"})
    svc._source_lifecycle.validate = AsyncMock(
        side_effect=SourceValidationError("notion", "invalid token")
    )

    obj_in = SourceConnectionCreate(
        short_name="notion",
        readable_collection_id="col-1",
        authentication=OAuthTokenAuthentication(access_token="invalid_token_12345"),
    )
    with pytest.raises(HTTPException) as exc_info:
        await svc._create_with_oauth_token(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())

    assert exc_info.value.status_code == 400
    detail = str(exc_info.value.detail).lower()
    assert "token" in detail
    assert "invalid" in detail


async def test_create_with_auth_provider_requires_provider_auth():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException, match="requires provider configuration"):
        await svc._create_with_auth_provider(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_auth_provider_not_found():
    svc = _service(_entry())
    svc._connection_repo.get_by_readable_id = AsyncMock(return_value=None)
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=AuthProviderAuthentication(provider_readable_id="missing"),
    )
    with pytest.raises(NotFoundException):
        await svc._create_with_auth_provider(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_auth_provider_rejects_unsupported_provider():
    svc = _service(_entry())
    svc._connection_repo.get_by_readable_id = AsyncMock(
        return_value=SimpleNamespace(short_name="unknown", readable_id="p1")
    )
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=AuthProviderAuthentication(provider_readable_id="provider-1"),
    )
    with pytest.raises(HTTPException, match="does not support"):
        await svc._create_with_auth_provider(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_oauth_browser_rejects_non_oauth_browser_auth():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"k": "v"}),
    )
    with pytest.raises(HTTPException, match="OAuth browser authentication expected"):
        await svc._create_with_oauth_browser(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_oauth_browser_rejects_non_oauth1_settings(monkeypatch):
    entry = _entry(oauth_type="oauth1")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(return_value=SimpleNamespace()),
    )
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException, match="is not OAuth1"):
        await svc._create_with_oauth_browser(AsyncMock(), obj_in=obj_in, entry=entry, ctx=_ctx())


async def test_create_with_oauth_browser_rejects_non_oauth2_settings(monkeypatch):
    entry = _entry(oauth_type="access_only")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(return_value=SimpleNamespace()),
    )
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException, match="is not OAuth2"):
        await svc._create_with_oauth_browser(AsyncMock(), obj_in=obj_in, entry=entry, ctx=_ctx())


async def test_create_with_oauth_browser_maps_oauth2_value_error_to_422(monkeypatch):
    entry = _entry(oauth_type="access_only")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    svc._extract_template_configs = MagicMock(return_value=None)
    svc._oauth2_service.generate_auth_url_with_redirect = AsyncMock(
        side_effect=ValueError("Template config fields missing or empty: subdomain")
    )
    from airweave.platform.auth.schemas import OAuth2Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth2Settings(
                integration_short_name="github",
                url="https://provider/authorize",
                backend_url="https://provider/token",
                grant_type="authorization_code",
                client_id="platform-client-id",
                client_secret="platform-client-secret",
                content_type="application/x-www-form-urlencoded",
                client_credential_location="payload",
            )
        ),
    )
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException) as exc_info:
        await svc._create_with_oauth_browser(AsyncMock(), obj_in=obj_in, entry=entry, ctx=_ctx())
    assert exc_info.value.status_code == 422


async def test_create_with_oauth_browser_rejects_missing_collection(monkeypatch):
    entry = _entry(oauth_type="access_only")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    svc._extract_template_configs = MagicMock(return_value=None)
    svc._oauth2_service.generate_auth_url_with_redirect = AsyncMock(
        return_value=("https://provider/auth", "verifier-123")
    )
    from airweave.platform.auth.schemas import OAuth2Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth2Settings(
                integration_short_name="github",
                url="https://provider/authorize",
                backend_url="https://provider/token",
                grant_type="authorization_code",
                client_id="platform-client-id",
                client_secret="platform-client-secret",
                content_type="application/x-www-form-urlencoded",
                client_credential_location="payload",
            )
        ),
    )
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="missing-col")
    with pytest.raises(NotFoundException, match="Collection not found"):
        await svc._create_with_oauth_browser(AsyncMock(), obj_in=obj_in, entry=entry, ctx=_ctx())


async def test_create_with_direct_auth_requires_direct_auth():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    with pytest.raises(HTTPException, match="Direct authentication requires credentials"):
        await svc._create_with_direct_auth(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())


async def test_create_with_direct_auth_delegates_to_authenticated_connection():
    svc = _service(_entry())
    svc._source_validation.validate_auth_schema = MagicMock(return_value=SimpleNamespace(model_dump=lambda: {"k": "v"}))
    svc._source_validation.validate_config = MagicMock(return_value={"cfg": "x"})
    svc._source_lifecycle.validate = AsyncMock()
    expected = MagicMock(id=uuid4())
    svc._create_authenticated_connection = AsyncMock(return_value=expected)

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"k": "v"}),
    )
    result = await svc._create_with_direct_auth(AsyncMock(), obj_in=obj_in, entry=_entry(), ctx=_ctx())
    assert result is expected


async def test_create_redirect_session_returns_created_id(monkeypatch):
    svc = _service(_entry())
    redirect_id = uuid4()
    monkeypatch.setattr(
        "airweave.domains.source_connections.create.crud.redirect_session.generate_unique_code",
        AsyncMock(return_value="abcd1234"),
    )
    monkeypatch.setattr(
        "airweave.domains.source_connections.create.crud.redirect_session.create",
        AsyncMock(return_value=SimpleNamespace(id=redirect_id)),
    )
    result = await svc._create_redirect_session(
        AsyncMock(),
        "https://provider/auth",
        _ctx(),
        SimpleNamespace(),
    )
    assert result == redirect_id


async def test_create_init_session_rejects_partial_custom_credentials():
    svc = _service(_entry())
    obj_in = SimpleNamespace(
        short_name="github",
        readable_collection_id="col-1",
        redirect_url=None,
        authentication=SimpleNamespace(client_id="only-id", client_secret=None),
        model_dump=lambda **kwargs: {
            "short_name": "github",
            "readable_collection_id": "col-1",
        },
    )
    with pytest.raises(HTTPException, match="both client_id and client_secret"):
        await svc._create_init_session(
            AsyncMock(),
            obj_in=obj_in,
            state="state",
            ctx=_ctx(),
            uow=SimpleNamespace(),
        )


async def test_create_init_session_sets_platform_default_overrides(monkeypatch):
    svc = _service(_entry())
    captured = {}

    async def _fake_create(*args, **kwargs):
        captured.update(kwargs["obj_in"])
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.crud.connection_init_session.create",
        _fake_create,
    )

    obj_in = SourceConnectionCreate(short_name="github", readable_collection_id="col-1")
    await svc._create_init_session(
        AsyncMock(),
        obj_in=obj_in,
        state="state",
        ctx=_ctx(),
        uow=SimpleNamespace(),
    )
    assert captured["overrides"]["oauth_client_mode"] == "platform_default"
    assert captured["overrides"]["client_id"] is None
    assert captured["overrides"]["client_secret"] is None


async def test_create_init_session_sets_byoc_nested_for_oauth1(monkeypatch):
    svc = _service(_entry())
    captured = {}

    async def _fake_create(*args, **kwargs):
        captured.update(kwargs["obj_in"])
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.crud.connection_init_session.create",
        _fake_create,
    )

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(consumer_key="ck", consumer_secret="cs"),
    )
    await svc._create_init_session(
        AsyncMock(),
        obj_in=obj_in,
        state="state",
        ctx=_ctx(),
        uow=SimpleNamespace(),
    )
    assert captured["overrides"]["oauth_client_mode"] == "byoc_nested"
    assert captured["overrides"]["client_id"] == "ck"
    assert captured["overrides"]["client_secret"] == "cs"

async def test_trigger_sync_workflow_publishes_event_before_workflow():
    svc = _service(_entry())
    call_order = []

    async def _publish(*args, **kwargs):
        call_order.append("publish")

    async def _run_workflow(*args, **kwargs):
        call_order.append("workflow")

    svc._event_bus.publish = AsyncMock(side_effect=_publish)
    svc._temporal_workflow_service.run_source_connection_workflow = AsyncMock(
        side_effect=_run_workflow
    )

    ctx = _ctx()
    sync_job_id = uuid4()
    sync_id = uuid4()
    sync_result = SimpleNamespace(
        sync_job=SimpleNamespace(id=sync_job_id),
        sync_id=sync_id,
        sync=SimpleNamespace(id=sync_id),
    )
    connection = SimpleNamespace(short_name="github")
    collection = SimpleNamespace(id=uuid4(), name="c", readable_id="col-1")

    await svc._trigger_sync_workflow(
        connection=connection,
        sync_result=sync_result,
        collection=collection,
        source_connection_id=uuid4(),
        ctx=ctx,
    )

    assert call_order == ["publish", "workflow"]


async def test_create_connection_record_preserves_connection_name():
    svc = _service(_entry())
    name = "My GitHub Connection"
    ctx = _ctx()

    await svc._create_connection_record(
        AsyncMock(),
        name=name,
        short_name="github",
        credential_id=None,
        ctx=ctx,
        uow=SimpleNamespace(),
    )

    create_call = svc._connection_repo._calls[-1]
    connection_create = create_call[2]
    assert connection_create.name == name
