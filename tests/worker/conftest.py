"""Worker tests default the ERROR debounce to a single observation.

The state sync historically declared ERROR on the first bad workload read,
and most tests here lean on that as a side effect while exercising something
else — recovery, subordinate workers, restart backoff. One sync must keep
meaning one declaration for them. The grace itself is pinned by
tests/worker/test_serve_manager_error_debounce.py, which overrides the
threshold in its own body.
"""

import pytest

from gpustack import envs


@pytest.fixture(autouse=True)
def _single_tick_error_debounce(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 1)
