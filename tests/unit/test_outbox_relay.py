import asyncio
from uuid import uuid4

import pytest

from app.application.errors import BrokerUnavailable, EventRejected
from app.application.outbox_relay import OutboxRelay
from app.application.results import RelayBatchResult
from app.domain.outbox import OutboxMessage
from app.entrypoints.relay import MAX_BACKOFF_SECONDS, Backoff, next_delay, run
from tests.unit.fakes import FakePublisher, FakeStore
from tests.unit.support import NOW, fixed_clock


async def _seed(store: FakeStore, count: int) -> None:
    async with store.uow() as uow:
        for _ in range(count):
            await uow.outbox.add(OutboxMessage.payment_created(uuid4()))
        await uow.commit()


def _relay(store: FakeStore, publisher: FakePublisher, batch_size: int = 10) -> OutboxRelay:
    return OutboxRelay(store.uow_factory(), publisher, batch_size, clock=fixed_clock)


class TestRunOnce:
    async def test_empty_outbox(self, store: FakeStore) -> None:
        result = await _relay(store, FakePublisher()).run_once()
        assert result == RelayBatchResult(fetched=0, published=0, failed=0, aborted=False)

    async def test_publishes_and_marks_the_whole_batch(self, store: FakeStore) -> None:
        await _seed(store, 3)
        publisher = FakePublisher()

        result = await _relay(store, publisher).run_once()

        assert result == RelayBatchResult(fetched=3, published=3, failed=0, aborted=False)
        assert [m.id for m in publisher.published] == [1, 2, 3]
        assert all(m.published_at == NOW and m.attempts == 0 for m in store.outbox_list())

    async def test_respects_batch_size(self, store: FakeStore) -> None:
        await _seed(store, 3)
        result = await _relay(store, FakePublisher(), batch_size=2).run_once()
        assert (result.fetched, result.published) == (2, 2)
        assert [m.published_at is None for m in store.outbox_list()] == [False, False, True]

    async def test_rejected_event_is_skipped_and_recorded(self, store: FakeStore) -> None:
        await _seed(store, 3)
        publisher = FakePublisher({2: EventRejected("no route")})

        result = await _relay(store, publisher).run_once()

        assert result == RelayBatchResult(fetched=3, published=2, failed=1, aborted=False)
        first, second, third = store.outbox_list()
        assert first.published_at == third.published_at == NOW
        assert second.published_at is None
        assert second.attempts == 1
        assert second.last_error is not None and "EventRejected" in second.last_error

    async def test_broker_outage_stops_the_batch_but_keeps_confirmed(
        self, store: FakeStore
    ) -> None:
        await _seed(store, 4)
        publisher = FakePublisher({2: BrokerUnavailable("down")})

        result = await _relay(store, publisher).run_once()

        assert result == RelayBatchResult(fetched=4, published=1, failed=1, aborted=True)
        messages = store.outbox_list()
        assert [m.published_at is not None for m in messages] == [True, False, False, False]
        assert [m.attempts for m in messages] == [0, 1, 0, 0]  # 3 and 4 not even attempted

    async def test_unexpected_error_still_commits_confirmed_events(self, store: FakeStore) -> None:
        # Without this, events 1 would be published again on every run.
        await _seed(store, 3)
        publisher = FakePublisher({2: RuntimeError("bug")})

        result = await _relay(store, publisher).run_once()

        assert result.aborted and result.published == 1
        first, second, _ = store.outbox_list()
        assert first.published_at == NOW
        assert second.attempts == 1
        assert second.last_error == "RuntimeError: bug"


class TestNextDelay:
    def test_full_batch_with_progress_continues_at_once(self) -> None:
        result = RelayBatchResult(fetched=10, published=10, failed=0, aborted=False)
        assert next_delay(result, 10, 0.5, Backoff(0.5)) == 0.0

    def test_partial_batch_waits_poll_interval(self) -> None:
        result = RelayBatchResult(fetched=3, published=3, failed=0, aborted=False)
        assert next_delay(result, 10, 0.5, Backoff(0.5)) == 0.5

    @pytest.mark.parametrize(
        "result",
        [
            pytest.param(RelayBatchResult(5, 1, 1, aborted=True), id="aborted"),
            pytest.param(RelayBatchResult(10, 0, 10, aborted=False), id="all-rejected"),
        ],
    )
    def test_no_progress_backs_off(self, result: RelayBatchResult) -> None:
        backoff = Backoff(0.5)
        delays = [next_delay(result, 10, 0.5, backoff) for _ in range(6)]
        assert delays == [0.5, 1.0, 2.0, 4.0, MAX_BACKOFF_SECONDS, MAX_BACKOFF_SECONDS]

    def test_success_resets_backoff(self) -> None:
        backoff = Backoff(0.5)
        failing = RelayBatchResult(5, 0, 1, aborted=True)
        next_delay(failing, 10, 0.5, backoff)
        next_delay(failing, 10, 0.5, backoff)
        next_delay(RelayBatchResult(1, 1, 0, aborted=False), 10, 0.5, backoff)
        assert next_delay(failing, 10, 0.5, backoff) == 0.5


async def test_run_stops_while_waiting(store: FakeStore) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(run(_relay(store, FakePublisher()), stop, poll_interval=60))
    await asyncio.sleep(0.05)  # now sleeping on an empty outbox
    stop.set()
    await asyncio.wait_for(task, timeout=2)  # must not wait for the 60 s poll interval


async def test_run_survives_iteration_errors(store: FakeStore) -> None:
    class BrokenRelay(OutboxRelay):
        calls = 0

        async def run_once(self) -> RelayBatchResult:
            BrokenRelay.calls += 1
            raise ConnectionError("database down")

    stop = asyncio.Event()
    relay = BrokenRelay(store.uow_factory(), FakePublisher(), 10)
    task = asyncio.create_task(run(relay, stop, poll_interval=0.01))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=6)
    assert BrokenRelay.calls >= 2  # kept going after the first failure
