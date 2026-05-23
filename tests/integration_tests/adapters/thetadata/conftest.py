"""Pytest configuration for ThetaData adapter tests.

ThetaData is a data-only adapter — no executions, no trader. Override the
autouse `trader` and `components` fixtures from the upstream adapters
conftest with no-ops so this suite doesn't have to stand up the full
exec/risk/portfolio chain (which expects exec_client / account_state /
instrument / venue fixtures that don't apply to data-only adapters).
"""

import asyncio

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")


@pytest.fixture
def event_loop():
    """Shim for upstream conftest's autouse cleanup_event_loop_tasks."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def trader():
    return None


@pytest.fixture(autouse=True)
def components():
    return None
