import pytest
from cryptography.fernet import InvalidToken

from core.services.encryption_service import EncryptionService


@pytest.mark.parametrize("plaintext", ["", "oauth-token", "Ключ 🔐 中文\n\x00"])
def test_encryption_round_trip_with_same_secret(plaintext):
    encrypted = EncryptionService("секрет 🔑").encrypt(data=plaintext)

    assert encrypted != plaintext
    assert EncryptionService("секрет 🔑").decrypt(data=encrypted) == plaintext


def test_encryption_uses_randomized_tokens():
    service = EncryptionService("secret")
    first = service.encrypt(data="token")
    second = service.encrypt(data="token")

    assert first != second
    assert service.decrypt(data=first) == service.decrypt(data=second) == "token"


@pytest.mark.parametrize("invalid_data", ["", "not-a-fernet-token", "поврежденный токен"])
def test_decryption_rejects_invalid_tokens(invalid_data):
    with pytest.raises(ValueError, match="Неверный ключ шифрования или поврежденные данные") as error:
        EncryptionService("secret").decrypt(data=invalid_data)

    assert isinstance(error.value.__cause__, InvalidToken)


def test_decryption_rejects_different_secret():
    encrypted = EncryptionService("original").encrypt(data="token")

    with pytest.raises(ValueError, match="Неверный ключ шифрования"):
        EncryptionService("different").decrypt(data=encrypted)
