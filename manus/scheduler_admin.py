"""Fixed Windows Task Scheduler operator controls for the PAPER wrapper.

This module registers, inspects, arms, disarms, or removes exactly one fixed
Windows task. It never runs the task. All Scheduler interactions use the one
absolute ``%SystemRoot%\\System32\\schtasks.exe`` executable through argument
arrays, a bounded timeout, closed stdin, and captured output.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as element_tree
from typing import Any, Callable, Mapping

from manus import paper_locks, scheduled_paper


SCHEDULER_ADMIN_VERSION = "scheduler-admin/v2"
TASK_NAME = "Phil Manus PAPER Hourly"
_SCHTASKS_TIMEOUT_SECONDS = 30
_TASK_XML_MAX_BYTES = 8192
_TASK_XML_NAMESPACE = "http://schemas.microsoft.com/windows/2004/02/mit/task"
_TASK_XML_PREFIX = ".phil-manus-paper-task."
_TASK_XML_SUFFIX = ".xml.tmp"
_OPERATIONS = ("status", "install", "enable", "disable", "uninstall")
_TASK_STATES = frozenset({"installed", "absent", "not-queried", "not-confirmed"})
_ACCOUNT_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$")
_REJECTED_CLI_MESSAGE = "REJECTED: fixed scheduled PAPER operation failed closed\n"


class SchedulerAdminError(RuntimeError):
    """Raised for a bounded, fail-closed scheduler operator failure."""


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _canonical_json(value: Any) -> str:
    return scheduled_paper._canonical_json(value)


def _validate_regular_path(path: pathlib.Path, label: str) -> pathlib.Path:
    candidate = pathlib.Path(path)
    try:
        if not candidate.is_absolute() or paper_locks.path_is_unsafe_indirection(candidate) or not candidate.is_file():
            raise OSError("unsafe fixed path")
    except OSError:
        raise SchedulerAdminError(f"Fixed {label} is unavailable") from None
    return candidate


def _fixed_schtasks_path(environment: Mapping[str, str]) -> pathlib.Path:
    system_root = environment.get("SystemRoot")
    if not isinstance(system_root, str) or not system_root:
        raise SchedulerAdminError("Fixed Task Scheduler utility is unavailable")
    root = pathlib.Path(system_root)
    try:
        if not root.is_absolute() or paper_locks.path_is_unsafe_indirection(root) or not root.is_dir():
            raise OSError("unsafe SystemRoot")
        system32 = root / "System32"
        if paper_locks.path_is_unsafe_indirection(system32) or not system32.is_dir():
            raise OSError("unsafe System32")
    except OSError:
        raise SchedulerAdminError("Fixed Task Scheduler utility is unavailable") from None
    return _validate_regular_path(system32 / "schtasks.exe", "Task Scheduler utility")


def _current_interactive_account(environment: Mapping[str, str]) -> str:
    domain = environment.get("USERDOMAIN")
    username = environment.get("USERNAME")
    if not isinstance(domain, str) or not isinstance(username, str):
        raise SchedulerAdminError("Current interactive Windows identity is unavailable")
    if not _ACCOUNT_COMPONENT_RE.fullmatch(domain) or not _ACCOUNT_COMPONENT_RE.fullmatch(username):
        raise SchedulerAdminError("Current interactive Windows identity is unavailable")
    return f"{domain}\\{username}"


def _fixed_launcher_path(repository_root: pathlib.Path) -> pathlib.Path:
    root = pathlib.Path(repository_root)
    try:
        if not root.is_absolute() or paper_locks.path_is_unsafe_indirection(root) or not root.is_dir():
            raise OSError("unsafe repository root")
        launcher = root / "scheduled_paper_task.py"
        launcher.relative_to(root)
    except (OSError, ValueError):
        raise SchedulerAdminError("Fixed scheduled PAPER launcher is unavailable") from None
    return _validate_regular_path(launcher, "scheduled PAPER launcher")


def _fixed_python_path(python_executable: str) -> pathlib.Path:
    executable = pathlib.Path(python_executable)
    try:
        if not executable.is_absolute() or not executable.is_file():
            raise OSError("invalid Python executable")
    except OSError:
        raise SchedulerAdminError("Fixed Python executable is unavailable") from None
    return executable


def _next_hour_boundary(now: Callable[[], dt.datetime]) -> str:
    value = now()
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SchedulerAdminError("Fixed scheduler clock is unavailable")
    local_value = value.astimezone()
    boundary = local_value.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
    return boundary.isoformat(timespec="seconds")


def _xml_tag(name: str) -> str:
    return f"{{{_TASK_XML_NAMESPACE}}}{name}"


def _xml_child(parent, name: str, text: str | None = None, **attributes):
    child = element_tree.SubElement(parent, _xml_tag(name), attributes)
    if text is not None:
        child.text = text
    return child


def _build_task_xml(
    *,
    account: str,
    python_executable: pathlib.Path,
    launcher: pathlib.Path,
    now: Callable[[], dt.datetime],
) -> bytes:
    """Build the one escaped InteractiveToken task definition with stdlib XML."""
    start_boundary = _next_hour_boundary(now)
    task = element_tree.Element(_xml_tag("Task"), {"version": "1.4"})
    registration = _xml_child(task, "RegistrationInfo")
    _xml_child(registration, "URI", f"\\{TASK_NAME}")

    triggers = _xml_child(task, "Triggers")
    trigger = _xml_child(triggers, "TimeTrigger")
    _xml_child(trigger, "StartBoundary", start_boundary)
    repetition = _xml_child(trigger, "Repetition")
    _xml_child(repetition, "Interval", "PT1H")

    principals = _xml_child(task, "Principals")
    principal = _xml_child(principals, "Principal", id="InteractiveUser")
    _xml_child(principal, "UserId", account)
    _xml_child(principal, "LogonType", "InteractiveToken")
    _xml_child(principal, "RunLevel", "LeastPrivilege")

    settings = _xml_child(task, "Settings")
    _xml_child(settings, "MultipleInstancesPolicy", "IgnoreNew")
    _xml_child(settings, "DisallowStartIfOnBatteries", "false")
    _xml_child(settings, "StopIfGoingOnBatteries", "false")
    _xml_child(settings, "AllowStartOnDemand", "false")
    _xml_child(settings, "AllowHardTerminate", "false")
    _xml_child(settings, "ExecutionTimeLimit", "PT0S")
    _xml_child(settings, "Enabled", "true")

    actions = _xml_child(task, "Actions", Context="InteractiveUser")
    action = _xml_child(actions, "Exec")
    _xml_child(action, "Command", str(python_executable))
    _xml_child(action, "Arguments", subprocess.list2cmdline([str(launcher)]))

    try:
        # Real Windows schtasks /Create rejects UTF-8 task XML with
        # "(1,40): unable to switch the encoding". Task Scheduler requires
        # UTF-16 little-endian bytes with a matching XML declaration; only the
        # encoding path changes, never the task semantics above.
        payload = element_tree.tostring(task, encoding="utf-16", xml_declaration=True)
    except (TypeError, ValueError, element_tree.ParseError):
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable") from None
    if len(payload) > _TASK_XML_MAX_BYTES:
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable")
    return payload


def _read_marker(*, scheduler_root: pathlib.Path | None) -> bool:
    try:
        return scheduled_paper.read_enable_marker(_scheduler_root=scheduler_root)
    except scheduled_paper.ScheduledPaperError:
        raise SchedulerAdminError("Fixed scheduler marker is unavailable") from None


def _write_marker(enabled: bool, *, scheduler_root: pathlib.Path | None) -> None:
    try:
        scheduled_paper.write_enable_marker(enabled, _scheduler_root=scheduler_root)
    except scheduled_paper.ScheduledPaperError:
        raise SchedulerAdminError("Fixed scheduler marker is unavailable") from None


def _resolve_fixed_scheduler_root(*, scheduler_root: pathlib.Path | None) -> pathlib.Path:
    try:
        return scheduled_paper.resolve_scheduler_root(_scheduler_root=scheduler_root)
    except scheduled_paper.ScheduledPaperError:
        raise SchedulerAdminError("Fixed scheduler root is unavailable") from None


def _write_task_xml(root: pathlib.Path, payload: bytes) -> pathlib.Path:
    """Create one bounded, fsynced, closed generated XML artifact under fixed root."""
    if not isinstance(payload, bytes) or not payload or len(payload) > _TASK_XML_MAX_BYTES:
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable")
    if payload[:2] != b"\xff\xfe":
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable")
    try:
        if paper_locks.path_is_unsafe_indirection(root) or not root.is_dir():
            raise OSError("unsafe scheduler root")
        descriptor, temporary_name = tempfile.mkstemp(prefix=_TASK_XML_PREFIX, suffix=_TASK_XML_SUFFIX, dir=root)
    except OSError:
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable") from None
    temporary_path = pathlib.Path(temporary_name)
    try:
        temporary_path.relative_to(root)
        if paper_locks.path_is_unsafe_indirection(temporary_path):
            raise OSError("unsafe generated task definition")
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable") from None
    return temporary_path


def _remove_task_xml(root: pathlib.Path, path: pathlib.Path) -> None:
    """Remove only the generated fixed-root installation artifact."""
    try:
        path.relative_to(root)
        if paper_locks.path_is_unsafe_indirection(path):
            raise OSError("unsafe generated task definition")
        path.unlink(missing_ok=True)
    except (OSError, ValueError):
        raise SchedulerAdminError("Fixed Task Scheduler definition cleanup failed") from None


def _run_schtasks(
    arguments: list[str],
    *,
    executable: pathlib.Path,
    _subprocess_run: Callable[..., Any] = subprocess.run,
) -> int:
    command = [str(executable), *arguments]
    try:
        completed = _subprocess_run(
            command,
            check=False,
            shell=False,
            stdin=subprocess.DEVNULL,
            timeout=_SCHTASKS_TIMEOUT_SECONDS,
            capture_output=True,
            text=False,
        )
        returncode = completed.returncode
    except Exception:
        raise SchedulerAdminError("Fixed Task Scheduler operation failed") from None
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise SchedulerAdminError("Fixed Task Scheduler operation failed")
    return returncode


def _query_task_state(*, environment: Mapping[str, str], _subprocess_run: Callable[..., Any]) -> str:
    """Return only locale-independent fixed-task query knowledge."""
    executable = _fixed_schtasks_path(environment)
    returncode = _run_schtasks(
        ["/Query", "/TN", TASK_NAME],
        executable=executable,
        _subprocess_run=_subprocess_run,
    )
    # schtasks emits localized output. A nonzero status alone does not prove
    # absence, so raw output is deliberately neither parsed nor exposed.
    return "installed" if returncode == 0 else "not-confirmed"


def _result_document(*, task_state: str, wrapper_enabled: bool) -> dict[str, Any]:
    if task_state not in _TASK_STATES or type(wrapper_enabled) is not bool:
        raise SchedulerAdminError("Fixed scheduler result is invalid")
    return {
        "scheduler_admin_version": SCHEDULER_ADMIN_VERSION,
        "task_name": TASK_NAME,
        "task_state": task_state,
        "wrapper_enabled": wrapper_enabled,
    }


def status(
    *,
    _scheduler_root: pathlib.Path | None = None,
    _environment: Mapping[str, str] | None = None,
    _subprocess_run: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Read fixed marker state and query only the one fixed task."""
    environment = os.environ if _environment is None else _environment
    wrapper_enabled = _read_marker(scheduler_root=_scheduler_root)
    task_state = _query_task_state(environment=environment, _subprocess_run=_subprocess_run)
    return _result_document(task_state=task_state, wrapper_enabled=wrapper_enabled)


def install(
    *,
    _scheduler_root: pathlib.Path | None = None,
    _environment: Mapping[str, str] | None = None,
    _subprocess_run: Callable[..., Any] = subprocess.run,
    _repository_root: Callable[[], pathlib.Path] = _repository_root,
    _python_executable: str = sys.executable,
    _now: Callable[[], dt.datetime] = lambda: dt.datetime.now().astimezone(),
    _xml_builder: Callable[..., bytes] = _build_task_xml,
    _xml_writer: Callable[[pathlib.Path, bytes], pathlib.Path] = _write_task_xml,
) -> dict[str, Any]:
    """Disarm first, then create the one fixed InteractiveToken task."""
    # This is deliberately the first production effect: all later validation or
    # Scheduler failures leave any stale enabled marker durably false.
    _write_marker(False, scheduler_root=_scheduler_root)

    environment = os.environ if _environment is None else _environment
    root = _resolve_fixed_scheduler_root(scheduler_root=_scheduler_root)
    executable = _fixed_schtasks_path(environment)
    launcher = _fixed_launcher_path(_repository_root())
    account = _current_interactive_account(environment)
    python_executable = _fixed_python_path(_python_executable)
    try:
        payload = _xml_builder(
            account=account,
            python_executable=python_executable,
            launcher=launcher,
            now=_now,
        )
    except SchedulerAdminError:
        raise
    except Exception:
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable") from None
    try:
        definition_path = _xml_writer(root, payload)
    except SchedulerAdminError:
        raise
    except Exception:
        raise SchedulerAdminError("Fixed Task Scheduler definition is unavailable") from None
    try:
        returncode = _run_schtasks(
            ["/Create", "/TN", TASK_NAME, "/XML", str(definition_path), "/F"],
            executable=executable,
            _subprocess_run=_subprocess_run,
        )
    finally:
        _remove_task_xml(root, definition_path)
    if returncode != 0:
        raise SchedulerAdminError("Fixed Task Scheduler installation failed")
    return _result_document(task_state="installed", wrapper_enabled=False)


def enable(
    *,
    _scheduler_root: pathlib.Path | None = None,
    _environment: Mapping[str, str] | None = None,
    _subprocess_run: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Arm future wrapper entry only after confirming the fixed task exists."""
    environment = os.environ if _environment is None else _environment
    _read_marker(scheduler_root=_scheduler_root)
    if _query_task_state(environment=environment, _subprocess_run=_subprocess_run) != "installed":
        raise SchedulerAdminError("Fixed Task Scheduler task is unavailable")
    _write_marker(True, scheduler_root=_scheduler_root)
    return _result_document(task_state="installed", wrapper_enabled=True)


def disable(*, _scheduler_root: pathlib.Path | None = None) -> dict[str, Any]:
    """Disarm future wrapper entry without querying, running, or deleting a task."""
    _write_marker(False, scheduler_root=_scheduler_root)
    return _result_document(task_state="not-queried", wrapper_enabled=False)


def uninstall(
    *,
    _scheduler_root: pathlib.Path | None = None,
    _environment: Mapping[str, str] | None = None,
    _subprocess_run: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Disarm first, then delete only the fixed scheduled task."""
    _write_marker(False, scheduler_root=_scheduler_root)
    environment = os.environ if _environment is None else _environment
    executable = _fixed_schtasks_path(environment)
    returncode = _run_schtasks(
        ["/Delete", "/TN", TASK_NAME, "/F"],
        executable=executable,
        _subprocess_run=_subprocess_run,
    )
    if returncode != 0:
        raise SchedulerAdminError("Fixed Task Scheduler removal failed")
    return _result_document(task_state="absent", wrapper_enabled=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Operate the one fixed disabled-by-default Windows PAPER task",
        allow_abbrev=False,
        add_help=False,
    )
    parser.add_argument("operation", choices=_OPERATIONS)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    # Reject raw argv before argparse can include user-controlled text in an
    # error message. The fixed CLI accepts exactly one fixed operation.
    if len(arguments) != 1 or arguments[0] not in _OPERATIONS:
        parser.exit(2, _REJECTED_CLI_MESSAGE)
    operations = {
        "status": status,
        "install": install,
        "enable": enable,
        "disable": disable,
        "uninstall": uninstall,
    }
    try:
        result = operations[arguments[0]]()
    except SchedulerAdminError:
        parser.exit(2, _REJECTED_CLI_MESSAGE)
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
