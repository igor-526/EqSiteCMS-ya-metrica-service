import base64
import hashlib
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

from core.entities.metrica_parse import ParseKind, ParseTask
from core.exceptions.peer import PeerServiceException
from core.schemas.clients.yandex_service import EncAccessToken, MetricaBinding
from core.services.encryption_service import EncryptionService
from core.services.parse_job_factory import ParseJobFactory
from core.services.parse_retry_policy import ParseExecutionError, RetryPolicy


@pytest.fixture
def env():
    task = ParseTask(
        run_id=uuid4(),
        equestrian_id=uuid4(),
        counter_id="123",
        yandex_account_id=uuid4(),
        kind=ParseKind.PAGES,
        date_from=date(2026, 10, 21),
        date_to=date(2026, 10, 25),
        sequence=1,
        max_attempts=3,
    )
    # Independent producer, matching the yandex-service SHA256 + Fernet wire format.
    cipher = Fernet(base64.urlsafe_b64encode(hashlib.sha256(b"shared-key").digest()))
    ciphertext = cipher.encrypt(b"TOKEN_SECRET").decode()
    binding = MetricaBinding.model_validate(
        {
            "counter": {
                "counter_id": task.counter_id,
                "equestrian_id": task.equestrian_id,
                "yandex_account": {"id": task.yandex_account_id},
            }
        }
    )
    client = SimpleNamespace(
        get_metrica_binding=AsyncMock(return_value=binding),
        get_enc_access_token=AsyncMock(return_value=EncAccessToken(enc_access_token=ciphertext)),
    )
    factory = ParseJobFactory(yandex_client=client, encryption=EncryptionService("shared-key"))
    return SimpleNamespace(**locals())


async def test_yandex_ciphertext_and_memory_only(env):
    job = await env.factory.create(task=env.task)
    assert job.access_token == "TOKEN_SECRET"
    assert "TOKEN_SECRET" not in repr(job)
    assert env.ciphertext not in repr(job)
    assert "token" not in env.task.model_dump_json()
    env.client.get_metrica_binding.assert_awaited_once_with(equestrian_id=env.task.equestrian_id)
    env.client.get_enc_access_token.assert_awaited_once_with(account_id=env.task.yandex_account_id)


@pytest.mark.parametrize("changed", ["missing", "counter", "account", "equestrian"])
async def test_binding_changed_before_credentials(env, changed):
    if changed == "missing":
        env.binding.counter = None
    elif changed == "counter":
        env.binding.counter.counter_id = "456"
    elif changed == "account":
        env.binding.counter.yandex_account.id = uuid4()
    else:
        env.binding.counter.equestrian_id = uuid4()
    with pytest.raises(ParseExecutionError) as exc:
        await env.factory.create(task=env.task)
    assert str(exc.value) == "binding_changed"
    env.client.get_enc_access_token.assert_not_awaited()


async def test_deleted_account(env):
    env.client.get_enc_access_token.side_effect = PeerServiceException("yandex", 400, "http")
    with pytest.raises(ParseExecutionError, match="credentials_unavailable"):
        await env.factory.create(task=env.task)


@pytest.mark.parametrize("ciphertext", ["broken-CIPHERTEXT", "other-key", "empty-token"])
async def test_decryption_failure(env, ciphertext):
    if ciphertext == "other-key":
        ciphertext = EncryptionService("different").encrypt(data="TOKEN_SECRET")
    elif ciphertext == "empty-token":
        ciphertext = env.cipher.encrypt(b"  ").decode()
    env.client.get_enc_access_token.return_value = EncAccessToken(enc_access_token=ciphertext)
    with pytest.raises(ParseExecutionError, match="credentials_invalid"):
        await env.factory.create(task=env.task)


@pytest.mark.parametrize("method", ["get_metrica_binding", "get_enc_access_token"])
async def test_peer_unavailable(env, method):
    getattr(env.client, method).side_effect = PeerServiceException("yandex", None, "timeout")
    with pytest.raises(PeerServiceException) as exc:
        await env.factory.create(task=env.task)
    decision = RetryPolicy().classify(exc.value)
    assert decision.error_code == "yandex_service_unavailable"
    assert decision.retryable
