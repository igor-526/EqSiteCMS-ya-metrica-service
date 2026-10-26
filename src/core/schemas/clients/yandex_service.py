from uuid import UUID

from pydantic import BaseModel, Field


class BindingAccount(BaseModel):
    id: UUID


class MetricaCounterBinding(BaseModel):
    counter_id: str = Field(min_length=1)
    equestrian_id: UUID
    yandex_account: BindingAccount

    @property
    def yandex_account_id(self) -> UUID:
        return self.yandex_account.id


class MetricaBinding(BaseModel):
    counter: MetricaCounterBinding | None


class EncAccessToken(BaseModel):
    enc_access_token: str = Field(min_length=1, repr=False)
