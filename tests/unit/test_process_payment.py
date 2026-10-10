from uuid import uuid4

import pytest

from app.application.errors import GatewayUnavailable, WebhookRejected, WebhookUnavailable
from app.application.process_payment import ProcessPayment
from app.domain.enums import PaymentStatus
from app.domain.exceptions import PaymentMissing
from app.domain.payment import Payment
from tests.unit.fakes import FakeGateway, FakeStore, FakeWebhook
from tests.unit.support import CMD, NOW, fixed_clock


async def _seed(store: FakeStore) -> Payment:
    payment = Payment.new(CMD, "hash", NOW)
    async with store.uow() as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.commit()
    return payment


def _process(store: FakeStore, gateway: FakeGateway, webhook: FakeWebhook) -> ProcessPayment:
    return ProcessPayment(store.uow_factory(), gateway, webhook, clock=fixed_clock)


async def test_pending_payment_is_processed_and_notified(store: FakeStore) -> None:
    payment = await _seed(store)
    gateway, webhook = FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhook()

    result = await _process(store, gateway, webhook).execute(payment.id)

    assert (result.status, result.processed_now, result.webhook_sent_now) == (
        PaymentStatus.SUCCEEDED,
        True,
        True,
    )
    [stored] = store.payment_list()
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at == NOW
    assert stored.webhook_sent_at == NOW
    # The webhook reports the stored state, including processed_at.
    [sent] = webhook.sent
    assert (sent.status, sent.processed_at) == (PaymentStatus.SUCCEEDED, NOW)


async def test_declined_payment_is_a_result_not_an_error(store: FakeStore) -> None:
    payment = await _seed(store)
    webhook = FakeWebhook()

    result = await _process(store, FakeGateway(PaymentStatus.FAILED), webhook).execute(payment.id)

    assert result.status is PaymentStatus.FAILED
    assert webhook.sent[0].status is PaymentStatus.FAILED


async def test_redelivery_after_success_does_nothing(store: FakeStore) -> None:
    payment = await _seed(store)
    gateway, webhook = FakeGateway(), FakeWebhook()
    process = _process(store, gateway, webhook)
    await process.execute(payment.id)

    again = await process.execute(payment.id)

    assert (again.processed_now, again.webhook_sent_now) == (False, False)
    assert len(gateway.calls) == 1
    assert len(webhook.sent) == 1


async def test_failed_webhook_retry_repeats_only_the_webhook(store: FakeStore) -> None:
    payment = await _seed(store)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook = FakeWebhook(WebhookUnavailable("503"))
    process = _process(store, gateway, webhook)

    with pytest.raises(WebhookUnavailable):
        await process.execute(payment.id)
    # The status is committed before the webhook: a retry continues from there.
    assert store.payment_list()[0].status is PaymentStatus.SUCCEEDED
    assert store.payment_list()[0].webhook_sent_at is None

    result = await process.execute(payment.id)

    assert (result.processed_now, result.webhook_sent_now) == (False, True)
    assert len(gateway.calls) == 1
    assert webhook.attempts == 2


async def test_lost_race_reports_the_stored_status(store: FakeStore) -> None:
    payment = await _seed(store)

    class RacingGateway(FakeGateway):
        async def process(self, payment: Payment) -> PaymentStatus:
            # Another delivery finishes first, with a different outcome.
            async with store.uow() as uow:
                await uow.payments.mark_processed(payment.id, PaymentStatus.FAILED, NOW)
                await uow.commit()
            return PaymentStatus.SUCCEEDED

    webhook = FakeWebhook()
    result = await _process(store, RacingGateway(), webhook).execute(payment.id)

    assert result.processed_now is False
    assert result.status is PaymentStatus.FAILED
    assert webhook.sent[0].status is PaymentStatus.FAILED


async def test_gateway_outage_leaves_payment_pending(store: FakeStore) -> None:
    payment = await _seed(store)
    webhook = FakeWebhook()

    with pytest.raises(GatewayUnavailable):
        await _process(store, FakeGateway(GatewayUnavailable("down")), webhook).execute(payment.id)

    assert store.payment_list()[0].status is PaymentStatus.PENDING
    assert webhook.attempts == 0


async def test_rejected_webhook_propagates(store: FakeStore) -> None:
    payment = await _seed(store)
    with pytest.raises(WebhookRejected):
        await _process(store, FakeGateway(), FakeWebhook(WebhookRejected("404"))).execute(
            payment.id
        )


async def test_unknown_payment_is_permanent_error(store: FakeStore) -> None:
    missing = uuid4()
    with pytest.raises(PaymentMissing) as exc_info:
        await _process(store, FakeGateway(), FakeWebhook()).execute(missing)
    assert exc_info.value.payment_id == missing
