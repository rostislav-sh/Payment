import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

from app.infrastructure.db.repositories import SqlOutboxRepository, SqlPaymentRepository
from tests.integration.support import UowFactory, make_payment

pytestmark = pytest.mark.integration


class Boom(Exception):
    pass


def _checked_out_connections(engine: AsyncEngine) -> int:
    pool = engine.pool
    assert isinstance(pool, QueuePool)
    return pool.checkedout()


async def test_exposes_repositories(uow_factory: UowFactory) -> None:
    async with uow_factory() as uow:
        assert isinstance(uow.payments, SqlPaymentRepository)
        assert isinstance(uow.outbox, SqlOutboxRepository)


async def test_commit_persists(uow_factory: UowFactory) -> None:
    payment = make_payment()
    async with uow_factory() as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.commit()

    async with uow_factory() as uow:
        assert await uow.payments.get_by_id(payment.id) == payment


async def test_leaving_without_commit_rolls_back(uow_factory: UowFactory) -> None:
    payment = make_payment()
    async with uow_factory() as uow:
        await uow.payments.insert_if_absent(payment)

    async with uow_factory() as uow:
        assert await uow.payments.get_by_id(payment.id) is None


async def test_explicit_rollback_discards_changes(uow_factory: UowFactory) -> None:
    payment = make_payment()
    async with uow_factory() as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.rollback()
        assert await uow.payments.get_by_id(payment.id) is None


async def test_exception_rolls_back_and_propagates(
    uow_factory: UowFactory, engine: AsyncEngine
) -> None:
    payment = make_payment()

    with pytest.raises(Boom):
        async with uow_factory() as uow:
            await uow.payments.insert_if_absent(payment)
            raise Boom

    # The session was closed and its connection returned to the pool.
    assert _checked_out_connections(engine) == 0
    async with uow_factory() as uow:
        assert await uow.payments.get_by_id(payment.id) is None


async def test_each_uow_uses_a_new_session(uow_factory: UowFactory) -> None:
    # Uncommitted work of one UoW is invisible to another one opened at the same time.
    payment = make_payment()
    async with uow_factory() as writer:
        await writer.payments.insert_if_absent(payment)
        async with uow_factory() as reader:
            assert await reader.payments.get_by_id(payment.id) is None
        await writer.commit()


async def test_unit_of_work_is_single_use(uow_factory: UowFactory) -> None:
    uow = uow_factory()
    async with uow:
        pass
    with pytest.raises(RuntimeError, match="single-use"):
        async with uow:
            pass


async def test_commit_outside_block_raises(uow_factory: UowFactory) -> None:
    with pytest.raises(RuntimeError, match="outside"):
        await uow_factory().commit()
