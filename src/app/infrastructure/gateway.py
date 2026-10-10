"""Emulated payment gateway: takes 2-5 s and approves 90% of payments (configurable)."""

import asyncio
import random

from app.domain.enums import PaymentStatus
from app.domain.payment import Payment


class EmulatedGateway:
    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        # Injectable for deterministic tests. Not for security: this only emulates outcomes.
        self._rng = rng or random.Random()

    async def process(self, payment: Payment) -> PaymentStatus:
        # A real gateway would receive payment.id as its idempotency key, so a repeated call
        # after a crash between charging and storing the result would not charge twice.
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
