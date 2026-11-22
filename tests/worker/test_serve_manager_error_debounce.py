"""One bad workload read is not a dead workload.

A containerd restart makes healthy workloads read as missing or UNKNOWN for a
few sync ticks while the containers themselves keep serving — the shims
survive the restart, so the first failed read is a property of the CRI, not of
the model. Declaring ERROR on that first read restarted every model on the
node at once. These pin the grace: a transient bad observation is held across
consecutive syncs, reset by a healthy one, and never accumulated for an
instance that is already ERROR; the runtime's own FAILED — a settled
diagnosis — declares immediately.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gpustack import envs
from gpustack.api.exceptions import NotFoundException
from gpustack.schemas.models import ModelInstanceStateEnum
from gpustack.worker.serve_manager import ServeManager
from gpustack_runtime.deployer import WorkloadStatusStateEnum
from tests.utils.model import new_model_instance


def _manager(worker_id: int = 1):
    clientset = MagicMock()
    clientset.model_instances.list.return_value = SimpleNamespace(items=[])
    clientset.cache_service_instances.list.return_value = SimpleNamespace(items=[])
    cfg = SimpleNamespace(log_dir="/tmp")
    manager = ServeManager(lambda: worker_id, lambda: clientset, cfg)
    manager._inference_backend_manager = MagicMock()
    return manager, clientset


def _instance(state=ModelInstanceStateEnum.RUNNING):
    return new_model_instance(1, "mi", 1, worker_id=1, state=state)


def _sync(manager, clientset, workload):
    """Run one state sync with the given workload read, and return the report
    mock so a test can assert on what (if anything) was declared."""
    clientset.model_instances.list.return_value = SimpleNamespace(items=[_instance()])
    with (
        patch("gpustack.worker.serve_manager.get_workload", return_value=workload),
        patch.object(manager, "_is_provisioning", return_value=False),
        # Reached only on a healthy read; the pop this test observes happens
        # before the model lookup, so raising here keeps the healthy path
        # short-circuited without faking a whole Model.
        patch.object(manager, "_get_model", side_effect=NotFoundException("no model")),
        patch.object(manager, "_ensure_container_log_persistence"),
        patch.object(manager, "_append_log_diagnosis", side_effect=lambda mi, msg: msg),
        patch.object(manager, "_report_model_instance") as report,
    ):
        manager.sync_model_instances_state()
    return report


# --- transient reads are held, not declared --------------------------------- #


def test_a_single_transient_read_does_not_declare_error(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 5)
    manager, _clientset = _manager()

    report = _sync(manager, _clientset, None)

    report.assert_not_called()


def test_error_is_declared_once_the_reads_persist(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 3)
    manager, clientset = _manager()

    _sync(manager, clientset, None).assert_not_called()
    _sync(manager, clientset, None).assert_not_called()
    report = _sync(manager, clientset, None)

    report.assert_called_once()
    assert report.call_args.kwargs["state"] == ModelInstanceStateEnum.ERROR
    assert "observed in 3 consecutive syncs" in report.call_args.kwargs["state_message"]


def test_a_healthy_read_resets_the_count(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 3)
    manager, clientset = _manager()
    healthy = SimpleNamespace(state=WorkloadStatusStateEnum.RUNNING)

    # Two missing reads, one healthy read, then two more: the healthy read
    # clears the tally, so the two that follow never reach the threshold.
    _sync(manager, clientset, None)
    _sync(manager, clientset, healthy)
    _sync(manager, clientset, None)
    report = _sync(manager, clientset, None)

    report.assert_not_called()


# --- FAILED is a settled diagnosis, not a transient read --------------------- #


def test_a_failed_workload_declares_error_immediately(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 5)
    manager, clientset = _manager()

    report = _sync(
        manager, clientset, SimpleNamespace(state=WorkloadStatusStateEnum.FAILED)
    )

    report.assert_called_once()
    assert report.call_args.kwargs["state"] == ModelInstanceStateEnum.ERROR


# --- an instance already in ERROR is left to the restart path ----------------- #


def test_an_instance_already_in_error_is_not_re_reported(monkeypatch):
    monkeypatch.setattr(envs, "MODEL_INSTANCE_ERROR_DEBOUNCE_TICKS", 3)
    manager, clientset = _manager()
    clientset.model_instances.list.return_value = SimpleNamespace(
        items=[_instance(state=ModelInstanceStateEnum.ERROR)]
    )

    with (
        patch("gpustack.worker.serve_manager.get_workload", return_value=None),
        patch.object(manager, "_is_provisioning", return_value=False),
        patch.object(manager, "_report_model_instance") as report,
    ):
        for _ in range(6):
            manager.sync_model_instances_state()

    report.assert_not_called()
    assert manager._workload_bad_observations == {}
