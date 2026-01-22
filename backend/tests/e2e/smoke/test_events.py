"""E2E tests for Events API and Webhook functionality.

These tests cover two distinct concepts:
- **Events (Messages)**: Records of what happened in the system (sync.pending, sync.completed, etc.)
- **Webhooks (Subscriptions)**: Endpoints that receive event notifications at configured URLs

Tests use the stub connector for fast execution while testing the full flow including Svix integration.

Test Categories:
- Tests WITHOUT @pytest.mark.svix: Test API functionality only
- Tests WITH @pytest.mark.svix: Require Svix to be running, but use Svix's API to verify delivery

These svix tests are skipped in CI because they require Svix to be running locally.
"""

import asyncio
import time
import uuid
from typing import AsyncGenerator, Dict

import httpx
import pytest
import pytest_asyncio


# Webhook delivery timeout
WEBHOOK_TIMEOUT = 30.0

# Dummy URL for webhook subscriptions - Svix will try to deliver here
# We don't need it to succeed, we just check Svix's message attempts
DUMMY_WEBHOOK_URL = "https://example.com/webhook"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


async def wait_for_sync_completed_message(
    api_client: httpx.AsyncClient,
    timeout: float = WEBHOOK_TIMEOUT,
) -> Dict:
    """Poll the messages API until a sync.completed message appears."""
    start_time = time.time()
    last_message_id = None

    while time.time() - start_time < timeout:
        response = await api_client.get(
            "/events/messages", params={"event_types": ["sync.completed"]}
        )
        if response.status_code == 200:
            messages = response.json()
            if messages:
                # Return the most recent message if it's new
                if messages[0]["id"] != last_message_id:
                    return messages[0]
                last_message_id = messages[0]["id"] if messages else None

        await asyncio.sleep(0.5)

    raise TimeoutError(f"No sync.completed message found within {timeout}s")




# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(scope="function")
def unique_webhook_url() -> str:
    """Generate a unique webhook URL for this test."""
    return f"https://example.com/webhook/{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture(scope="function")
async def webhook_subscription(
    api_client: httpx.AsyncClient,
    unique_webhook_url: str,
) -> AsyncGenerator[Dict, None]:
    """Create a webhook subscription for sync.completed events."""
    response = await api_client.post(
        "/events/subscriptions",
        json={
            "url": unique_webhook_url,
            "event_types": ["sync.completed"],
        },
    )
    assert response.status_code == 200, f"Failed to create subscription: {response.text}"
    subscription = response.json()

    yield subscription

    # Cleanup
    try:
        await api_client.delete(f"/events/subscriptions/{subscription['id']}")
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def webhook_subscription_all_events(
    api_client: httpx.AsyncClient,
    unique_webhook_url: str,
) -> AsyncGenerator[Dict, None]:
    """Create a webhook subscription for all sync event types."""
    response = await api_client.post(
        "/events/subscriptions",
        json={
            "url": unique_webhook_url,
            "event_types": [
                "sync.pending",
                "sync.running",
                "sync.completed",
                "sync.failed",
                "sync.cancelled",
            ],
        },
    )
    assert response.status_code == 200, f"Failed to create subscription: {response.text}"
    subscription = response.json()

    yield subscription

    # Cleanup
    try:
        await api_client.delete(f"/events/subscriptions/{subscription['id']}")
    except Exception:
        pass


# =============================================================================
# EVENTS TESTS - Testing the Events API (messages)
# =============================================================================


@pytest.mark.asyncio
class TestEventsMessages:
    """Tests for event messages - the record of what events occurred."""

    async def test_get_messages_returns_list(self, api_client: httpx.AsyncClient):
        """Test that GET /events/messages returns a list."""
        response = await api_client.get("/events/messages")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_get_messages_with_event_type_filter(
        self, api_client: httpx.AsyncClient
    ):
        """Test filtering messages by event type."""
        response = await api_client.get(
            "/events/messages", params={"event_types": ["sync.completed"]}
        )
        assert response.status_code == 200
        messages = response.json()

        # All returned messages should be sync.completed
        for msg in messages:
            assert msg["eventType"] == "sync.completed"

    @pytest.mark.svix
    async def test_messages_created_after_sync(
        self,
        api_client: httpx.AsyncClient,
        collection: Dict,
    ):
        """Test that event messages are created when syncs occur."""
        # Get initial message count
        initial_response = await api_client.get(
            "/events/messages", params={"event_types": ["sync.completed"]}
        )
        initial_count = len(initial_response.json()) if initial_response.status_code == 200 else 0

        # Trigger a sync
        response = await api_client.post(
            "/source-connections",
            json={
                "name": "Stub Message Test",
                "description": "Testing message creation",
                "short_name": "stub",
                "readable_collection_id": collection["readable_id"],
                "authentication": {"credentials": {"stub_key": "key"}},
                "config": {"entity_count": "1"},
                "sync_immediately": True,
            },
        )
        assert response.status_code == 200

        # Wait for new message to appear
        message = await wait_for_sync_completed_message(api_client, timeout=WEBHOOK_TIMEOUT)

        # Verify message structure
        assert "id" in message
        assert "eventType" in message
        assert message["eventType"] == "sync.completed"
        assert "payload" in message


@pytest.mark.asyncio
@pytest.mark.svix
class TestEventTypes:
    """Tests for different event types (sync.pending, sync.running, sync.completed, etc.)."""

    async def test_event_payload_structure(
        self,
        api_client: httpx.AsyncClient,
        collection: Dict,
    ):
        """Test that event payloads contain all required fields."""
        # Trigger a sync
        await api_client.post(
            "/source-connections",
            json={
                "name": "Stub Payload Test",
                "description": "Testing payload structure",
                "short_name": "stub",
                "readable_collection_id": collection["readable_id"],
                "authentication": {"credentials": {"stub_key": "key"}},
                "config": {"entity_count": "1"},
                "sync_immediately": True,
            },
        )

        # Wait for message
        message = await wait_for_sync_completed_message(api_client, timeout=WEBHOOK_TIMEOUT)
        payload = message.get("payload", {})

        # Verify required fields per SyncEventPayload schema
        assert "event_type" in payload
        assert "job_id" in payload
        assert "collection_readable_id" in payload
        assert "collection_name" in payload
        assert "source_type" in payload
        assert "status" in payload
        assert "timestamp" in payload

    async def test_completed_event_has_job_id(
        self,
        api_client: httpx.AsyncClient,
        collection: Dict,
    ):
        """Test that completed events include the job_id in correct format."""
        # Trigger a sync
        await api_client.post(
            "/source-connections",
            json={
                "name": "Stub Job ID Test",
                "description": "Testing job_id presence",
                "short_name": "stub",
                "readable_collection_id": collection["readable_id"],
                "authentication": {"credentials": {"stub_key": "key"}},
                "config": {"entity_count": "2"},
                "sync_immediately": True,
            },
        )

        # Wait for message
        message = await wait_for_sync_completed_message(api_client, timeout=WEBHOOK_TIMEOUT)
        payload = message.get("payload", {})

        assert "job_id" in payload
        job_id = payload["job_id"]
        # UUID format validation
        assert len(job_id) == 36  # xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx


# =============================================================================
# WEBHOOK TESTS - Testing webhook subscriptions
# =============================================================================


@pytest.mark.asyncio
class TestWebhookSubscriptions:
    """Tests for webhook subscription CRUD operations."""

    async def test_list_subscriptions(self, api_client: httpx.AsyncClient):
        """Test listing all webhook subscriptions."""
        response = await api_client.get("/events/subscriptions")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_create_subscription(
        self, api_client: httpx.AsyncClient, unique_webhook_url: str
    ):
        """Test creating a webhook subscription."""
        response = await api_client.post(
            "/events/subscriptions",
            json={
                "url": unique_webhook_url,
                "event_types": ["sync.completed"],
            },
        )
        assert response.status_code == 200
        subscription = response.json()
        assert subscription["id"] is not None
        assert subscription["url"].rstrip("/") == unique_webhook_url.rstrip("/")

        # Cleanup
        await api_client.delete(f"/events/subscriptions/{subscription['id']}")

    async def test_create_subscription_multiple_event_types(
        self, api_client: httpx.AsyncClient, unique_webhook_url: str
    ):
        """Test creating a subscription with multiple event types."""
        event_types = ["sync.completed", "sync.failed", "sync.running"]

        response = await api_client.post(
            "/events/subscriptions",
            json={
                "url": unique_webhook_url,
                "event_types": event_types,
            },
        )
        assert response.status_code == 200
        subscription = response.json()

        # Cleanup
        await api_client.delete(f"/events/subscriptions/{subscription['id']}")

    async def test_get_subscription_by_id(
        self, api_client: httpx.AsyncClient, webhook_subscription: Dict
    ):
        """Test getting a specific subscription with its delivery attempts."""
        response = await api_client.get(
            f"/events/subscriptions/{webhook_subscription['id']}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["endpoint"]["id"] == webhook_subscription["id"]
        assert "message_attempts" in data

    async def test_update_subscription_url(
        self,
        api_client: httpx.AsyncClient,
        webhook_subscription: Dict,
    ):
        """Test updating a subscription URL."""
        new_url = f"https://example.com/webhook/updated-{uuid.uuid4().hex[:8]}"
        response = await api_client.patch(
            f"/events/subscriptions/{webhook_subscription['id']}",
            json={"url": new_url},
        )
        assert response.status_code == 200
        updated = response.json()
        assert updated["url"].rstrip("/") == new_url.rstrip("/")

    async def test_delete_subscription(
        self, api_client: httpx.AsyncClient, unique_webhook_url: str
    ):
        """Test deleting a webhook subscription."""
        # Create a subscription to delete
        create_response = await api_client.post(
            "/events/subscriptions",
            json={
                "url": unique_webhook_url,
                "event_types": ["sync.completed"],
            },
        )
        subscription = create_response.json()

        # Delete it
        delete_response = await api_client.delete(
            f"/events/subscriptions/{subscription['id']}"
        )
        assert delete_response.status_code == 200

    async def test_get_subscription_secret(
        self, api_client: httpx.AsyncClient, webhook_subscription: Dict
    ):
        """Test retrieving the signing secret for a subscription."""
        response = await api_client.get(
            f"/events/subscriptions/{webhook_subscription['id']}/secret"
        )
        assert response.status_code == 200
        secret_data = response.json()
        assert "key" in secret_data
        # Svix secrets start with whsec_
        assert secret_data["key"].startswith("whsec_")
