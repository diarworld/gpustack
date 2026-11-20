"""
Ensure the local gpustack package is imported before any installed ones.
"""

import logging
import os
import sys

# Prepend the repository root to sys.path so that the local gpustack module is used
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Several server modules import fcntl (unix-only). The stub keeps the test
# suite runnable on Windows dev machines; every real platform has the module.
try:
    import fcntl  # noqa: F401
except ImportError:  # pragma: no cover - Windows only
    import types

    _fcntl = types.ModuleType("fcntl")
    _fcntl.LOCK_EX, _fcntl.LOCK_NB, _fcntl.LOCK_SH, _fcntl.LOCK_UN = 2, 4, 1, 8
    _fcntl.flock = lambda *args, **kwargs: None
    _fcntl.lockf = lambda *args, **kwargs: None
    sys.modules["fcntl"] = _fcntl

# The app registers a custom TRACE level at startup (gpustack.logging.setup_logging);
# tests don't run that, so ``logger.trace(...)`` calls on the ActiveRecord write path
# (e.g. delete_cache_by_key) would raise AttributeError. Register just the method here
# — it is a no-op at the default level, so it adds no log noise or other side effects.
from gpustack.logging import TRACE_LEVEL, trace  # noqa: E402

logging.addLevelName(TRACE_LEVEL, "TRACE")
if not hasattr(logging.Logger, "trace"):
    logging.Logger.trace = trace
