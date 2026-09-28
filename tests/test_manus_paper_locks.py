"""Cross-process regressions for fixed-root Manus PAPER locks."""

from __future__ import annotations

import multiprocessing
import os
import pathlib
import tempfile
import time
import unittest

from manus import paper_locks


def _hold_cycle_lock(root: str, ready, release) -> None:
    with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=pathlib.Path(root)):
        ready.set()
        release.wait(10)


def _exit_while_holding_cycle_lock(root: str, ready) -> None:
    lock = paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=pathlib.Path(root))
    ready.set()
    # Process exit, rather than PaperLock.release(), is the property under test.
    os._exit(0)


def _attempt_cycle_lock(root: str, result) -> None:
    """Report a second process's nonblocking ownership attempt."""
    try:
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=pathlib.Path(root)):
            result.put("acquired")
    except paper_locks.LockUnavailableError:
        result.put("unavailable")


class PaperLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = pathlib.Path(self.temporary_directory.name) / "external-locks"
        self.context = multiprocessing.get_context("spawn")

    def _start_holder(self):
        ready = self.context.Event()
        release = self.context.Event()
        process = self.context.Process(target=_hold_cycle_lock, args=(str(self.root), ready, release))
        process.start()
        self.assertTrue(ready.wait(10), "holder process did not acquire the lock")
        self.addCleanup(self._stop, process, release)
        return process, release

    @staticmethod
    def _stop(process, release) -> None:
        release.set()
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(10)

    def test_second_process_cannot_acquire_while_first_holds_lock(self):
        process, _ = self._start_holder()
        with self.assertRaises(paper_locks.LockUnavailableError):
            paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.root)
        self.assertTrue(process.is_alive())

    def test_release_permits_next_process_to_acquire(self):
        process, release = self._start_holder()
        release.set()
        process.join(10)
        self.assertEqual(process.exitcode, 0)
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.root) as lock:
            self.assertEqual(lock.name, "cycle")
            self.assertTrue(lock.path.exists())

    def test_process_exit_releases_authoritative_os_lock(self):
        ready = self.context.Event()
        process = self.context.Process(target=_exit_while_holding_cycle_lock, args=(str(self.root), ready))
        process.start()
        self.assertTrue(ready.wait(10), "exit holder did not acquire the lock")
        process.join(10)
        self.assertEqual(process.exitcode, 0)
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.root):
            pass

    def test_malformed_or_stale_metadata_cannot_bypass_os_lock(self):
        path = self.root / "cycle.lock"
        path.parent.mkdir(parents=True)
        path.write_text("malformed stale metadata\n", encoding="utf-8")
        # The pre-existing metadata is never parsed as ownership authority: a
        # normal acquisition succeeds despite it. Do not overwrite a live lock
        # file here; Windows msvcrt intentionally locks byte zero exclusively.
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.root):
            result = self.context.Queue()
            process = self.context.Process(target=_attempt_cycle_lock, args=(str(self.root), result))
            process.start()
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(10)
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(result.get(timeout=5), "unavailable")
        # Release permits a fresh holder; stale metadata cannot retain or break
        # authority after the OS-held lock is gone.
        self.assertEqual(process.exitcode, 0)
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.root):
            pass

    def test_fixed_logical_locks_are_distinct_and_validate_names(self):
        request_hash = "a" * 64
        intent_id = "123e4567-e89b-42d3-a456-426614174000"
        with paper_locks.acquire_request_lock(request_hash, _lock_root=self.root) as request_lock, \
             paper_locks.acquire_application_lock(intent_id, _lock_root=self.root) as application_lock, \
             paper_locks.acquire_journal_writer_lock(_lock_root=self.root) as journal_lock:
            self.assertNotEqual(request_lock.path, application_lock.path)
            self.assertNotEqual(application_lock.path, journal_lock.path)
        with self.assertRaises(paper_locks.LockError):
            paper_locks.acquire_request_lock("not-a-hash", _lock_root=self.root)
        with self.assertRaises(paper_locks.LockError):
            paper_locks.acquire_application_lock("not-a-uuid", _lock_root=self.root)

    def test_distinct_request_hashes_do_not_block_each_other(self):
        with paper_locks.acquire_request_lock("a" * 64, _lock_root=self.root):
            with paper_locks.acquire_request_lock("b" * 64, _lock_root=self.root) as second:
                self.assertEqual(second.name, f"request-{'b' * 64}")

    def test_bounded_wait_times_out_without_breaking_lock(self):
        process, _ = self._start_holder()
        started = time.monotonic()
        with self.assertRaises(paper_locks.LockUnavailableError):
            paper_locks.acquire_cycle_lock(nonblocking=False, timeout_seconds=0.05, _lock_root=self.root)
        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(process.is_alive())


if __name__ == "__main__":
    unittest.main()
