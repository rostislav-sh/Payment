import pytest

from tests.unit.fakes import FakeStore
from tests.unit.support import fixed_clock


@pytest.fixture
def store() -> FakeStore:
    return FakeStore(clock=fixed_clock)
