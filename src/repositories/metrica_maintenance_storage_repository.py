"""Idempotent ledger storage. Caller commits inbox+ledger together or rolls back.

Uses the arbitration tenant lock; no transport, stats effects or internal commits.
"""

import json
from collections.abc import Mapping, Sequence
from hashlib import sha256
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from core.entities.metrica_maintenance import (
    MaintenanceConflictError,
    MaintenanceLease,
    MaintenanceLeaseLostError,
    WorkKind,
)
from models import metrica_maintenance_chunks as chunks
from models import metrica_maintenance_inbox as inbox
from models import metrica_maintenance_operations as operations
from models import metrica_tenant_gates as gates
from repositories.metrica_maintenance_repository import MaintenanceArbitrationRepository


def payload_hash(payload: object) -> str:
    """Hash validated JSON only; envelope validation/UUID serialization is upstream."""
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class MaintenanceStorageRepository(MaintenanceArbitrationRepository):
    async def _operation(self, tenant: UUID, operation: UUID) -> Mapping:
        await self.lock_scope([tenant])
        row = (
            (await self.session.execute(select(operations).where(operations.c.id == operation)))
            .mappings()
            .one_or_none()
        )
        if row is None or row["equestrian_id"] != tenant:
            raise MaintenanceConflictError("Unknown operation or tenant mismatch")
        return row

    async def save_operation(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        kind: WorkKind,
        cleanup_seq: int | None = None,
        binding_generation: int | None = None,
        normalization_revision: int | None = None,
    ) -> bool:
        if kind == WorkKind.CLEANUP:
            valid = (
                cleanup_seq is not None
                and cleanup_seq > 0  # noqa: W503
                and binding_generation is not None  # noqa: W503
                and binding_generation >= 0  # noqa: W503
                and normalization_revision is None  # noqa: W503
            )
        else:
            valid = (
                kind == WorkKind.MERGE
                and normalization_revision is not None  # noqa: W503
                and normalization_revision > 0  # noqa: W503
                and cleanup_seq is None  # noqa: W503
                and binding_generation is None  # noqa: W503
            )
        if not valid:
            raise ValueError("Invalid operation identity")
        await self.lock_scope([tenant])
        await self.session.execute(insert(gates).values(equestrian_id=tenant).on_conflict_do_nothing())
        identity = dict(
            id=operation,
            equestrian_id=tenant,
            kind=kind,
            cleanup_seq=cleanup_seq,
            binding_generation=binding_generation,
            normalization_revision=normalization_revision,
        )
        created = await self.session.scalar(
            insert(operations)
            .values(**identity, status="pending" if kind == WorkKind.CLEANUP else "collecting")
            .on_conflict_do_nothing()
            .returning(operations.c.id)
        )
        if created is None:
            row = await self._operation(tenant, operation)
            if any(row[key] != value for key, value in identity.items()):
                raise MaintenanceConflictError("Operation identity conflict")
        if kind == WorkKind.CLEANUP:
            await self.session.execute(
                update(gates)
                .where(gates.c.equestrian_id == tenant)
                .values(
                    cleanup_requested_seq=func.greatest(gates.c.cleanup_requested_seq, cleanup_seq),
                    binding_generation=func.greatest(gates.c.binding_generation, binding_generation),
                    updated_at=func.clock_timestamp(),
                )
            )
        return created is not None

    async def save_inbox(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        consumer: str,
        message_id: UUID,
        payload: dict,
    ) -> bool:
        if not consumer or len(consumer) > 128:
            raise ValueError("Invalid consumer")
        digest = payload_hash(payload)
        await self._operation(tenant, operation)
        inserted = await self.session.scalar(
            insert(inbox)
            .values(
                consumer=consumer,
                message_id=message_id,
                equestrian_id=tenant,
                operation_id=operation,
                payload_hash=digest,
            )
            .on_conflict_do_nothing()
            .returning(inbox.c.message_id)
        )
        if inserted is None:
            row = (
                (
                    await self.session.execute(
                        select(inbox).where(
                            inbox.c.consumer == consumer,
                            inbox.c.message_id == message_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
            if (row["equestrian_id"], row["operation_id"], row["payload_hash"]) != (tenant, operation, digest):
                raise MaintenanceConflictError("Inbox identity or payload conflict")
        return inserted is not None

    @staticmethod
    def _mappings(mappings: Sequence[dict[str, str]]) -> list[dict[str, str]]:
        if not 1 <= len(mappings) <= 500:
            raise ValueError("Invalid mapping count")
        result = []
        sources = set()
        for pair in mappings:
            if set(pair) != {"source_page_id", "target_page_id"}:
                raise ValueError("Invalid mapping fields")
            source, target = UUID(pair["source_page_id"]), UUID(pair["target_page_id"])
            if source == target or source in sources:
                raise ValueError("Invalid or duplicated source")
            sources.add(source)
            result.append({"source_page_id": str(source), "target_page_id": str(target)})
        # List order is part of the producer's immutable payload identity.
        if len(json.dumps(result).encode()) > 256 * 1024:
            raise ValueError("Mapping body exceeds limit")
        return result

    async def save_chunk(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        part_index: int,
        mappings: Sequence[dict[str, str]],
    ) -> bool:
        if part_index < 0:
            raise ValueError("Invalid part index")
        body = self._mappings(mappings)
        digest = payload_hash(body)
        row = await self._operation(tenant, operation)
        if row["kind"] != WorkKind.MERGE or (row["part_count"] is not None and part_index >= row["part_count"]):
            raise MaintenanceConflictError("Chunk does not belong to sealed merge")
        previous = (
            await self.session.execute(
                select(chunks.c.payload_hash).where(
                    chunks.c.operation_id == operation,
                    chunks.c.equestrian_id == tenant,
                    chunks.c.part_index == part_index,
                )
            )
        ).scalar_one_or_none()
        if previous is not None:
            if previous != digest:
                raise MaintenanceConflictError("Chunk payload conflict")
            return False
        if row["status"] != "collecting" and row["cleanup_operation_id"] is None:
            raise MaintenanceConflictError("Unexpected new chunk after collection")
        # Late parts of a superseded operation are retained for correlation, never executed.
        await self.session.execute(
            insert(chunks).values(
                equestrian_id=tenant,
                operation_id=operation,
                part_index=part_index,
                payload_hash=digest,
                mappings=body,
            )
        )
        await self._ready(tenant, operation)
        return True

    async def seal(self, tenant: UUID, operation: UUID, *, part_count: int) -> bool:
        if part_count < 0:
            raise ValueError("Invalid part count")
        row = await self._operation(tenant, operation)
        if row["kind"] != WorkKind.MERGE or row["part_count"] not in (None, part_count):
            raise MaintenanceConflictError("Seal identity conflict")
        if row["part_count"] is not None:
            return False
        out_of_range = await self.session.scalar(
            select(func.count())
            .select_from(chunks)
            .where(
                chunks.c.operation_id == operation,
                chunks.c.equestrian_id == tenant,
                chunks.c.part_index >= part_count,
            )
        )
        if out_of_range:
            raise MaintenanceConflictError("Seal excludes stored chunks")
        await self.session.execute(
            update(operations)
            .where(
                operations.c.id == operation,
                operations.c.equestrian_id == tenant,
            )
            .values(part_count=part_count, updated_at=func.clock_timestamp())
        )
        await self._ready(tenant, operation)
        return True

    async def _ready(self, tenant: UUID, operation: UUID) -> None:
        count = (
            select(func.count())
            .select_from(chunks)
            .where(
                chunks.c.operation_id == operation,
                chunks.c.equestrian_id == tenant,
            )
            .scalar_subquery()
        )
        await self.session.execute(
            update(operations)
            .where(
                operations.c.id == operation,
                operations.c.equestrian_id == tenant,
                operations.c.status == "collecting",
                operations.c.part_count == count,
            )
            .values(status="waiting", updated_at=func.clock_timestamp())
        )

    async def finish_work(
        self,
        lease: MaintenanceLease,
        *,
        status: str,
        result_message_id: UUID | None = None,
    ) -> None:
        """Fenced terminal work CAS; stats effects and result outbox belong to caller."""
        allowed = {
            WorkKind.CLEANUP: {"completed", "failed_blocked"},
            WorkKind.MERGE: {"awaiting_seo", "failed_blocked"},
        }
        if status not in allowed.get(lease.kind, set()) or (
            (status != "failed_blocked") != (result_message_id is not None)
        ):
            raise ValueError("Invalid completion transition")
        await self.assert_owned(lease)
        if status == "awaiting_seo":
            row = await self._operation(lease.equestrian_id, lease.operation_id)
            finished = await self.session.scalar(
                select(func.count())
                .select_from(chunks)
                .where(
                    chunks.c.operation_id == lease.operation_id,
                    chunks.c.equestrian_id == lease.equestrian_id,
                    chunks.c.status == "completed",
                )
            )
            if row["part_count"] is None or finished != row["part_count"]:
                raise MaintenanceConflictError("Merge collection or execution is incomplete")
        changed = await self.session.scalar(
            update(operations)
            .where(*self._owned(lease))
            .values(
                status=status,
                result_message_id=result_message_id,
                lease_owner=None,
                lease_expires_at=None,
                fencing_revision=operations.c.fencing_revision + 1,
                updated_at=func.clock_timestamp(),
            )
            .returning(operations.c.id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Lease expired before completion")
        if lease.kind == WorkKind.CLEANUP and status == "completed":
            row = await self._operation(lease.equestrian_id, lease.operation_id)
            advanced = await self.session.scalar(
                update(gates)
                .where(
                    gates.c.equestrian_id == lease.equestrian_id,
                    gates.c.cleanup_completed_seq == row["cleanup_seq"] - 1,
                )
                .values(cleanup_completed_seq=row["cleanup_seq"], updated_at=func.clock_timestamp())
                .returning(gates.c.equestrian_id)
            )
            if advanced is None:
                raise MaintenanceConflictError("Cleanup prefix is not contiguous")

    async def supersede(self, tenant: UUID, operation: UUID, *, cleanup_operation: UUID) -> bool:
        """Revoke a merge between batches; caller atomically persists superseded result."""
        row = await self._operation(tenant, operation)
        cleanup = await self._operation(tenant, cleanup_operation)
        if row["kind"] != WorkKind.MERGE or cleanup["kind"] != WorkKind.CLEANUP:
            raise MaintenanceConflictError("Invalid supersession correlation")
        if row["cleanup_operation_id"] is not None:
            # A later delete never rewrites the cleanup that SEO is already awaiting.
            return False
        if row["status"] == "completed":
            return False
        await self.session.execute(
            update(operations)
            .where(
                operations.c.id == operation,
                operations.c.equestrian_id == tenant,
            )
            .values(
                status="superseded_by_delete",
                cleanup_operation_id=cleanup_operation,
                lease_owner=None,
                lease_expires_at=None,
                fencing_revision=operations.c.fencing_revision + 1,
                result_message_id=None,
                updated_at=func.clock_timestamp(),
            )
        )
        return True

    async def finalize(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        normalization_revision: int,
        result_message_id: UUID,
    ) -> bool:
        row = await self._operation(tenant, operation)
        if (
            row["kind"] != WorkKind.MERGE
            or row["normalization_revision"] != normalization_revision  # noqa: W503
            or row["result_message_id"] != result_message_id  # noqa: W503
            or row["status"] not in {"awaiting_seo", "completed"}  # noqa: W503
        ):
            raise MaintenanceConflictError("Finalized correlation conflict")
        if row["status"] == "completed":
            return False
        if row["cleanup_operation_id"] is not None:
            cleanup = await self._operation(tenant, row["cleanup_operation_id"])
            if cleanup["status"] != "completed" or row["part_count"] is None:
                raise MaintenanceConflictError("Superseded merge is not ready for finalization")
        await self.session.execute(
            update(operations)
            .where(
                operations.c.id == operation,
                operations.c.equestrian_id == tenant,
            )
            .values(status="completed", updated_at=func.clock_timestamp())
        )
        return True

    async def record_superseded_result(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        result_message_id: UUID,
    ) -> bool:
        """Caller writes correlated result outbox before committing this transition."""
        row = await self._operation(tenant, operation)
        if row["kind"] != WorkKind.MERGE or row["cleanup_operation_id"] is None:
            raise MaintenanceConflictError("Operation is not superseded")
        if row["result_message_id"] is not None:
            if row["result_message_id"] != result_message_id:
                raise MaintenanceConflictError("Superseded result identity conflict")
            return False
        if row["status"] != "superseded_by_delete":
            raise MaintenanceConflictError("Invalid superseded state")
        await self.session.execute(
            update(operations)
            .where(
                operations.c.id == operation,
                operations.c.equestrian_id == tenant,
            )
            .values(status="awaiting_seo", result_message_id=result_message_id, updated_at=func.clock_timestamp())
        )
        return True

    async def checkpoint_chunk(
        self,
        lease: MaintenanceLease,
        *,
        part_index: int,
        checkpoint: dict,
        completed: bool,
        lease_seconds: int,
    ) -> None:
        if lease.kind != WorkKind.MERGE or part_index < 0:
            raise ValueError("Invalid chunk checkpoint")
        self._duration(lease_seconds)
        await self.assert_owned(lease)
        changed = await self.session.scalar(
            update(chunks)
            .where(
                chunks.c.equestrian_id == lease.equestrian_id,
                chunks.c.operation_id == lease.operation_id,
                chunks.c.part_index == part_index,
                chunks.c.status != "completed",
            )
            .values(
                checkpoint=checkpoint,
                status="completed" if completed else "running",
                updated_at=func.clock_timestamp(),
            )
            .returning(chunks.c.part_index)
        )
        if changed is None:
            raise MaintenanceConflictError("Unknown or completed chunk checkpoint")
        await self.checkpoint(lease, checkpoint={"part_index": part_index}, lease_seconds=lease_seconds)
