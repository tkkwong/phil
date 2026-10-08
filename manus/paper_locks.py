"""Fixed-root, standard-library cross-process locks for the Manus PAPER workflow.

The OS-held file lock is authoritative. A lock file's JSON metadata is merely
an operator diagnostic and is never read to decide ownership, expiry, or
whether a process may break a lock. OS process exit releases the authoritative
fcntl/msvcrt lock automatically.

Production callers select one of the fixed logical locks below; they never
supply an arbitrary lock path. Private underscore root overrides exist only for
offline tests with temporary external directories.
"""

from __future__ import annotations

import datetime as dt
import errno
import hashlib
import json
import os
import pathlib
import re
import socket
import stat
import time
import uuid
from dataclasses import dataclass
from typing import Callable


_LOCK_ROOT_CHILDREN = ("phil-manus", "locks")
_LOCK_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,160}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class LockError(RuntimeError):
    """Raised when a fixed lock root or OS lock cannot be used safely."""


class LockUnavailableError(LockError):
    """Raised when another process currently owns an authoritative OS lock."""


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def path_is_unsafe_indirection(path: pathlib.Path) -> bool:
    """Return whether an existing path is a symlink or detectable reparse point.

    The standard library exposes ordinary symlinks on every supported platform
    and exposes Windows file attributes as ``st_file_attributes``.  On Windows,
    a directory junction/reparse point therefore fails closed without ctypes,
    pywin32, shell, or subprocess dependencies.  This guards the fixed local
    authority paths; it is not a security boundary against another process
    running under the same Windows identity.
    """
    candidate = pathlib.Path(path)
    try:
        metadata = candidate.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    if candidate.is_symlink():
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return bool(reparse_flag and isinstance(attributes, int) and attributes & reparse_flag)


def resolve_lock_root() -> pathlib.Path:
    """Return the one production lock root outside the checked-out repository."""
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        raise LockError("Fixed Manus lock root is unavailable")
    root = pathlib.Path(local_appdata) / _LOCK_ROOT_CHILDREN[0] / _LOCK_ROOT_CHILDREN[1]
    try:
        root.resolve(strict=False).relative_to(_repository_root())
    except ValueError:
        return root
    raise LockError("Fixed Manus lock root must be outside the repository")


def _validate_test_root(root: pathlib.Path | None) -> pathlib.Path:
    """Resolve a private test seam while preserving the external-root boundary."""
    candidate = resolve_lock_root() if root is None else pathlib.Path(root)
    try:
        candidate.resolve(strict=False).relative_to(_repository_root())
    except ValueError:
        return candidate
    raise LockError("Fixed Manus lock root must be outside the repository")


def _prepare_root(root: pathlib.Path) -> pathlib.Path:
    """Create/check the fixed root; same-identity tampering is not a trust claim."""
    try:
        for directory in (root.parent, root):
            if path_is_unsafe_indirection(directory):
                raise OSError("unsafe fixed lock root")
            if directory.exists() and not directory.is_dir():
                raise OSError("invalid fixed lock root")
        root.mkdir(parents=True, exist_ok=True)
        if not root.is_dir() or path_is_unsafe_indirection(root):
            raise OSError("invalid lock root")
    except OSError:
        raise LockError("Fixed Manus lock root is unavailable") from None
    return root


def _lock_path(name: str, root: pathlib.Path) -> pathlib.Path:
    if not _LOCK_NAME_RE.fullmatch(name):
        raise LockError("Fixed Manus lock name is invalid")
    path = root / f"{name}.lock"
    try:
        path.relative_to(root)
    except ValueError:
        raise LockError("Fixed Manus lock path is unavailable") from None
    if path_is_unsafe_indirection(path):
        raise LockError("Fixed Manus lock path is unavailable")
    return path


def _utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _try_os_lock(handle) -> bool:
    """Try exactly one nonblocking exclusive lock on the opened file handle."""
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, 13, 36}:
                return False
            raise LockError("Fixed Manus OS lock is unavailable") from exc
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EAGAIN}:
            return False
        raise LockError("Fixed Manus OS lock is unavailable") from exc


def _unlock_os_lock(handle) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass
class PaperLock:
    """One acquired fixed lock held by an open OS file descriptor."""

    _handle: object
    path: pathlib.Path
    name: str
    purpose: str
    _released: bool = False

    def release(self) -> None:
        """Release the advisory OS lock and close its authoritative descriptor."""
        if self._released:
            return
        try:
            _unlock_os_lock(self._handle)
        except OSError:
            # Closing still releases an OS-held lock. Do not turn cleanup into a
            # false success claim for a caller's protected transaction.
            pass
        finally:
            self._handle.close()
            self._released = True

    def __enter__(self) -> "PaperLock":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self.release()
        return False


def _write_informational_metadata(handle, *, name: str, purpose: str) -> None:
    """Write non-authoritative diagnostics only after the OS lock is held."""
    document = {
        "acquired_at": _utc_timestamp(),
        "hostname": socket.gethostname()[:255],
        "pid": os.getpid(),
        "purpose": purpose,
        "lock_name": name,
    }
    encoded = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    handle.seek(0)
    handle.truncate()
    handle.write(encoded)
    handle.flush()
    os.fsync(handle.fileno())


def _acquire(
    name: str,
    *,
    purpose: str,
    nonblocking: bool,
    timeout_seconds: float | None,
    _lock_root: pathlib.Path | None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> PaperLock:
    """Acquire a fixed lock using only OS state as the ownership authority."""
    if not isinstance(nonblocking, bool):
        raise LockError("Fixed Manus lock acquisition mode is invalid")
    if timeout_seconds is not None and (
        isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 0
    ):
        raise LockError("Fixed Manus lock timeout is invalid")
    if nonblocking and timeout_seconds not in (None, 0):
        raise LockError("Nonblocking fixed Manus lock cannot have a wait timeout")

    root = _prepare_root(_validate_test_root(_lock_root))
    path = _lock_path(name, root)
    try:
        handle = path.open("a+b")
    except OSError:
        raise LockError("Fixed Manus lock path is unavailable") from None

    # msvcrt locks a byte range. Establish a harmless first byte before taking
    # the lock; metadata is never authority and will be rewritten only after
    # successful lock acquisition.
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        handle.close()
        raise LockError("Fixed Manus lock path is unavailable") from None

    deadline = None if timeout_seconds is None else monotonic() + float(timeout_seconds)
    while True:
        if _try_os_lock(handle):
            try:
                _write_informational_metadata(handle, name=name, purpose=purpose)
            except OSError:
                try:
                    _unlock_os_lock(handle)
                except OSError:
                    pass
                handle.close()
                raise LockError("Fixed Manus lock metadata is unavailable") from None
            return PaperLock(handle, path, name, purpose)
        if nonblocking:
            handle.close()
            raise LockUnavailableError(f"Fixed Manus lock already held: {name}")
        if deadline is not None:
            remaining = deadline - monotonic()
            if remaining <= 0:
                handle.close()
                raise LockUnavailableError(f"Timed out waiting for fixed Manus lock: {name}")
            sleep(min(0.05, remaining))
        else:
            sleep(0.05)


def _uuid_lock_component(intent_id: str) -> str:
    if not isinstance(intent_id, str):
        raise LockError("Fixed application lock intent id is invalid")
    try:
        parsed = uuid.UUID(intent_id)
    except (AttributeError, ValueError) as exc:
        raise LockError("Fixed application lock intent id is invalid") from exc
    if parsed.version != 4 or str(parsed) != intent_id:
        raise LockError("Fixed application lock intent id is invalid")
    return intent_id


def acquire_cycle_lock(
    *,
    nonblocking: bool = True,
    timeout_seconds: float | None = None,
    _lock_root: pathlib.Path | None = None,
) -> PaperLock:
    """Acquire the reusable fixed future-cycle lock; no scheduler is provided."""
    return _acquire(
        "cycle",
        purpose="manus-paper-cycle",
        nonblocking=nonblocking,
        timeout_seconds=timeout_seconds,
        _lock_root=_lock_root,
    )


def acquire_request_lock(
    request_sha256: str,
    *,
    nonblocking: bool = True,
    timeout_seconds: float | None = None,
    _lock_root: pathlib.Path | None = None,
) -> PaperLock:
    """Acquire the fixed lock for exactly one canonical request fingerprint."""
    if not isinstance(request_sha256, str) or not _SHA256_RE.fullmatch(request_sha256):
        raise LockError("Fixed research request lock hash is invalid")
    return _acquire(
        f"request-{request_sha256}",
        purpose="manus-research-request",
        nonblocking=nonblocking,
        timeout_seconds=timeout_seconds,
        _lock_root=_lock_root,
    )


def acquire_application_lock(
    intent_id: str,
    *,
    nonblocking: bool = True,
    timeout_seconds: float | None = None,
    _lock_root: pathlib.Path | None = None,
) -> PaperLock:
    """Acquire the fixed lock for one canonical staged intent application."""
    return _acquire(
        f"application-{_uuid_lock_component(intent_id)}",
        purpose="manus-paper-apply",
        nonblocking=nonblocking,
        timeout_seconds=timeout_seconds,
        _lock_root=_lock_root,
    )


def acquire_journal_writer_lock(
    *,
    nonblocking: bool = True,
    timeout_seconds: float | None = None,
    _lock_root: pathlib.Path | None = None,
) -> PaperLock:
    """Acquire the shared fixed Manus PAPER journal-writer boundary."""
    return _acquire(
        "journal-writer",
        purpose="manus-paper-journal-writer",
        nonblocking=nonblocking,
        timeout_seconds=timeout_seconds,
        _lock_root=_lock_root,
    )


def acquire_provenance_writer_lock(
    *,
    nonblocking: bool = True,
    timeout_seconds: float | None = None,
    _lock_root: pathlib.Path | None = None,
) -> PaperLock:
    """Acquire the fixed append-only decision-provenance writer boundary."""
    return _acquire(
        "provenance-writer",
        purpose="manus-decision-provenance",
        nonblocking=nonblocking,
        timeout_seconds=timeout_seconds,
        _lock_root=_lock_root,
    )
