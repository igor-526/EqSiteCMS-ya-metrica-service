from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from core.entities.metrica_maintenance import BindingLifecycle


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
    binding_generation: int = Field(ge=0, strict=True)
    cleanup_requested_seq: int = Field(ge=0, strict=True)
    cleanup_completed_seq: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> MetricaBinding:
        if self.cleanup_completed_seq > self.cleanup_requested_seq:
            raise ValueError("Invalid cleanup watermark")
        if self.counter is not None and self.binding_generation == 0:
            raise ValueError("Existing binding requires a generation")
        return self

    def lifecycle(self) -> BindingLifecycle:
        return BindingLifecycle(
            binding_generation=self.binding_generation,
            cleanup_requested_seq=self.cleanup_requested_seq,
            cleanup_completed_seq=self.cleanup_completed_seq,
            account_id=self.counter.yandex_account_id if self.counter else None,
            counter_id=self.counter.counter_id if self.counter else None,
        )


class EncAccessToken(BaseModel):
    enc_access_token: str = Field(min_length=1, repr=False)
