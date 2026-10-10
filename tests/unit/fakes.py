"""In-memory implementations of the application ports (app.application.ports) for unit tests.

Semantics follow the SQL implementations:

* a ``FakeUnitOfWork`` records writes as operations (inserts and guarded row updates); they
  reach the shared ``FakeStore`` only on ``commit()``, where each update is re-applied to the
  latest committed row, so updates of different units of work never overwrite each other's
  columns and ``attempts + 1`` never loses an increment;
* other units of work see only committed data (READ COMMITTED);
* a unique violation (payment id, idempotency_key, outbox (event_type, aggregate_id)) raises
  ``FakeIntegrityError`` and, like an SQLAlchemy session, leaves the unit of work unusable
  until ``rollback()``;
* outbox ids come from a sequence that is never rolled back (like IDENTITY);
* every value crossing the boundary is deep-copied.

Not modelled: row locks. Postgres would make a second writer wait and re-check its WHERE
clause; here, if a guarded update no longer holds at commit, ``FakeConcurrencyError`` is
raised instead. So concurrent scenarios (races, SKIP LOCKED) belong in integration tests.
"""

import copy
import itertools
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from types import TracebackType
from typing import Self
from uuid import UUID

from app.application.clock import Clock, utc_now
from app.application.ports.gateway import PaymentGateway
from app.application.ports.publisher import EventPublisher
from app.application.ports.uow import UnitOfWorkFactory
from app.application.ports.webhook import WebhookSender
from app.domain.enums import FINAL_STATUSES, PaymentStatus
from app.domain.exceptions import InvalidStatusTransition
from app.domain.outbox import OutboxMessage
from app.domain.payment import Payment
from app.infrastructure.db.repositories import MAX_ERROR_LENGTH


class FakeIntegrityError(Exception):
    """Unique constraint violation (stands in for sqlalchemy.exc.IntegrityError)."""


class FakePendingRollbackError(Exception):
    """The unit of work failed earlier (stands in for sqlalchemy.exc.PendingRollbackError)."""


class FakeConcurrencyError(Exception):
    """A guarded update lost a race with another unit of work (row locks are not modelled)."""


@dataclass(frozen=True, slots=True)
class _Update[K, T]:
    key: K
    guard: Callable[[T], bool]
    change: Callable[[T], T]


@dataclass
class FakeStore:
    """Committed state shared by all units of work created from it."""

    payments: dict[UUID, Payment] = field(default_factory=dict)
    outbox: dict[int, OutboxMessage] = field(default_factory=dict)
    commits: int = 0
    clock: Clock = utc_now  # outbox.created_at (a DB default in SQL)
    _outbox_ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    def uow(self) -> FakeUnitOfWork:
        return FakeUnitOfWork(self)

    def uow_factory(self) -> UnitOfWorkFactory:
        return self.uow

    def payment_list(self) -> list[Payment]:
        return [copy.deepcopy(p) for p in self.payments.values()]

    def outbox_list(self) -> list[OutboxMessage]:
        return [copy.deepcopy(self.outbox[i]) for i in sorted(self.outbox)]

    def next_outbox_id(self) -> int:
        return next(self._outbox_ids)


def _apply[K, T](
    rows: dict[K, T], inserts: dict[K, T], updates: Sequence[_Update[K, T]], strict: bool
) -> dict[K, T]:
    """Rows after this transaction's inserts and updates. ``strict``: fail on a lost race."""
    result = {**rows, **inserts}
    for update in updates:
        row = result.get(update.key)
        if row is None or not update.guard(row):
            if strict:
                raise FakeConcurrencyError(f"row {update.key!r} changed by another unit of work")
            continue
        result[update.key] = update.change(row)
    return result


def _check_unique[T](rows: Iterable[T], key: Callable[[T], object], what: str) -> None:
    seen: set[object] = set()
    for row in rows:
        value = key(row)
        if value in seen:
            raise FakeIntegrityError(f"duplicate {what}: {value!r}")
        seen.add(value)


class FakeUnitOfWork:
    """Single-use, like SqlUnitOfWork."""

    def __init__(self, store: FakeStore) -> None:
        self._store = store
        self._payment_inserts: dict[UUID, Payment] = {}
        self._payment_updates: list[_Update[UUID, Payment]] = []
        self._outbox_inserts: dict[int, OutboxMessage] = {}
        self._outbox_updates: list[_Update[int, OutboxMessage]] = []
        self._state = "new"  # new -> active -> closed
        self._failed = False
        self._payments = FakePaymentRepository(self)
        self._outbox = FakeOutboxRepository(self)

    @property
    def payments(self) -> FakePaymentRepository:
        return self._payments

    @property
    def outbox(self) -> FakeOutboxRepository:
        return self._outbox

    async def __aenter__(self) -> Self:
        if self._state != "new":
            raise RuntimeError("FakeUnitOfWork is single-use; create a new one")
        self._state = "active"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._discard()
        self._state = "closed"

    async def commit(self) -> None:
        self.check_usable()
        try:
            payments = _apply(
                self._store.payments, self._payment_inserts, self._payment_updates, strict=True
            )
            outbox = _apply(
                self._store.outbox, self._outbox_inserts, self._outbox_updates, strict=True
            )
            # Re-check unique indexes against what other units of work committed meanwhile.
            _check_unique(payments.values(), lambda p: p.idempotency_key, "idempotency_key")
            _check_unique(outbox.values(), lambda m: (m.event_type, m.aggregate_id), "outbox event")
        except Exception:
            self._failed = True
            raise
        self._store.payments, self._store.outbox = payments, outbox
        self._store.commits += 1
        self._discard()

    async def rollback(self) -> None:
        self._ensure_active()
        self._discard()
        self._failed = False

    # --- used by the repositories -------------------------------------------------------

    def check_usable(self) -> None:
        self._ensure_active()
        if self._failed:
            raise FakePendingRollbackError("a previous statement failed; call rollback()")

    def fail(self, error: FakeIntegrityError) -> FakeIntegrityError:
        self._failed = True
        return error

    def visible_payments(self) -> dict[UUID, Payment]:
        return _apply(
            self._store.payments, self._payment_inserts, self._payment_updates, strict=False
        )

    def visible_outbox(self) -> dict[int, OutboxMessage]:
        return _apply(self._store.outbox, self._outbox_inserts, self._outbox_updates, strict=False)

    def insert_payment(self, payment: Payment) -> None:
        self._payment_inserts[payment.id] = copy.deepcopy(payment)

    def update_payment(
        self, key: UUID, guard: Callable[[Payment], bool], change: Callable[[Payment], Payment]
    ) -> None:
        self._payment_updates.append(_Update(key, guard, change))

    def insert_outbox(self, message: OutboxMessage) -> None:
        assert message.id is not None
        self._outbox_inserts[message.id] = copy.deepcopy(message)

    def update_outbox(
        self,
        key: int,
        guard: Callable[[OutboxMessage], bool],
        change: Callable[[OutboxMessage], OutboxMessage],
    ) -> None:
        self._outbox_updates.append(_Update(key, guard, change))

    @property
    def store(self) -> FakeStore:
        return self._store

    def _discard(self) -> None:
        self._payment_inserts.clear()
        self._payment_updates.clear()
        self._outbox_inserts.clear()
        self._outbox_updates.clear()

    def _ensure_active(self) -> None:
        if self._state != "active":
            raise RuntimeError("FakeUnitOfWork is used outside of 'async with'")


def _always[T](_: T) -> bool:
    return True


class FakePaymentRepository:
    def __init__(self, uow: FakeUnitOfWork) -> None:
        self._uow = uow

    async def insert_if_absent(self, payment: Payment) -> bool:
        self._uow.check_usable()
        visible = self._uow.visible_payments()
        if any(p.idempotency_key == payment.idempotency_key for p in visible.values()):
            return False
        if payment.id in visible:
            raise self._uow.fail(FakeIntegrityError(f"duplicate payment id: {payment.id}"))
        self._uow.insert_payment(payment)
        return True

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        self._uow.check_usable()
        return copy.deepcopy(self._uow.visible_payments().get(payment_id))

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        self._uow.check_usable()
        found = (p for p in self._uow.visible_payments().values() if p.idempotency_key == key)
        return copy.deepcopy(next(found, None))

    async def mark_processed(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        self._uow.check_usable()
        if status not in FINAL_STATUSES:
            raise InvalidStatusTransition(f"mark_processed needs a final status, got {status}")
        return self._update(
            payment_id,
            guard=lambda p: p.status is PaymentStatus.PENDING,
            change=lambda p: replace(p, status=status, processed_at=processed_at),
        )

    async def mark_webhook_sent(self, payment_id: UUID, sent_at: datetime) -> bool:
        self._uow.check_usable()
        return self._update(
            payment_id,
            guard=lambda p: p.webhook_sent_at is None,
            change=lambda p: replace(p, webhook_sent_at=sent_at),
        )

    def _update(
        self,
        payment_id: UUID,
        guard: Callable[[Payment], bool],
        change: Callable[[Payment], Payment],
    ) -> bool:
        current = self._uow.visible_payments().get(payment_id)
        if current is None or not guard(current):
            return False
        self._uow.update_payment(payment_id, guard, change)
        return True


class FakeOutboxRepository:
    def __init__(self, uow: FakeUnitOfWork) -> None:
        self._uow = uow

    async def add(self, message: OutboxMessage) -> None:
        self._uow.check_usable()
        # Taken before the unique check: IDENTITY values are consumed even by failed inserts.
        message_id = self._uow.store.next_outbox_id()
        key = (message.event_type, message.aggregate_id)
        if any((m.event_type, m.aggregate_id) == key for m in self._uow.visible_outbox().values()):
            raise self._uow.fail(FakeIntegrityError(f"duplicate outbox event: {key!r}"))
        self._uow.insert_outbox(replace(message, id=message_id, created_at=self._uow.store.clock()))

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        self._uow.check_usable()
        visible = self._uow.visible_outbox()
        ids = [i for i in sorted(visible) if visible[i].published_at is None][:limit]
        return [copy.deepcopy(visible[i]) for i in ids]

    async def mark_published(self, ids: Sequence[int], published_at: datetime) -> None:
        self._uow.check_usable()
        for message_id in ids:
            if message_id in self._uow.visible_outbox():
                self._uow.update_outbox(
                    message_id, _always, lambda m: replace(m, published_at=published_at)
                )

    async def register_failure(self, message_id: int, error: str) -> None:
        self._uow.check_usable()
        if message_id in self._uow.visible_outbox():
            self._uow.update_outbox(
                message_id,
                _always,
                lambda m: replace(m, attempts=m.attempts + 1, last_error=error[:MAX_ERROR_LENGTH]),
            )


# Static check (mypy): the fakes satisfy the application ports.
_port_check: UnitOfWorkFactory = FakeStore().uow_factory()


# --- other ports ----------------------------------------------------------------------------


class FakePublisher:
    """EventPublisher: records published messages; ``failures`` maps outbox id -> exception."""

    def __init__(self, failures: dict[int, Exception] | None = None) -> None:
        self.published: list[OutboxMessage] = []
        self.failures = failures or {}

    async def publish(self, message: OutboxMessage) -> None:
        assert message.id is not None
        if message.id in self.failures:
            raise self.failures[message.id]
        self.published.append(message)


class FakeGateway:
    """PaymentGateway with scripted outcomes (or exceptions), counting calls."""

    def __init__(self, *outcomes: PaymentStatus | Exception) -> None:
        self._outcomes = list(outcomes) or [PaymentStatus.SUCCEEDED]
        self.calls: list[UUID] = []

    async def process(self, payment: Payment) -> PaymentStatus:
        self.calls.append(payment.id)
        outcome = self._outcomes.pop(0) if len(self._outcomes) > 1 else self._outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeWebhook:
    """WebhookSender with scripted failures (consumed one per call), recording deliveries."""

    def __init__(self, *failures: Exception) -> None:
        self._failures = list(failures)
        self.sent: list[Payment] = []
        self.attempts = 0

    async def send(self, payment: Payment) -> None:
        self.attempts += 1
        if self._failures:
            raise self._failures.pop(0)
        self.sent.append(copy.deepcopy(payment))


# Static checks (mypy): the fakes satisfy the ports.
_publisher_check: EventPublisher = FakePublisher()
_gateway_check: PaymentGateway = FakeGateway()
_webhook_check: WebhookSender = FakeWebhook()
