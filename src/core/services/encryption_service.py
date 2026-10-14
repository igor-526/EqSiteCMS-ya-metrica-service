import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken


class EncryptionService:
    def __init__(self, secret_key: str) -> None:
        digest = hashlib.sha256(secret_key.encode("utf-8")).digest()
        fernet_key = base64.urlsafe_b64encode(digest)
        self._cipher = Fernet(fernet_key)

    def encrypt(self, *, data: str) -> str:
        encrypted_bytes = self._cipher.encrypt(data.encode("utf-8"))
        return encrypted_bytes.decode("utf-8")

    def decrypt(self, *, data: str) -> str:
        try:
            decrypted_bytes = self._cipher.decrypt(data.encode("utf-8"))
            return decrypted_bytes.decode("utf-8")
        except InvalidToken as ex:
            raise ValueError("Неверный ключ шифрования или поврежденные данные") from ex
