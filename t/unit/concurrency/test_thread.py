import operator
import time
from unittest.mock import Mock, patch

import pytest

from celery.utils.functional import noop


class test_thread_TaskPool:

    def test_on_apply(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
        finally:
            x.stop()

    def test_info(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            assert x.info
        finally:
            x.stop()

    def test_on_stop(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        x.on_stop()
        with pytest.raises(RuntimeError):
            x.on_apply(operator.add, (2, 2), {}, noop, noop)

    def test_on_stop_cancels_pending_futures(self):
        import threading

        from celery.concurrency import thread

        x = thread.TaskPool(limit=1)

        started = threading.Event()
        shutdown = threading.Event()

        def blocking_task():
            started.set()
            shutdown.wait(timeout=5)

        stop_thread = None
        try:
            # Submit a long-running task to occupy the single thread
            x.on_apply(blocking_task, (), {}, noop, noop)

            # Wait until the first task is actually running
            assert started.wait(timeout=5), "Timed out waiting for blocking_task to start"

            # Submit another task — guaranteed to be pending
            result = x.on_apply(noop, (), {}, noop, noop)

            # Stop the pool in a background thread — should cancel the pending future
            def _run_on_stop():
                x.on_stop()

            stop_thread = threading.Thread(target=_run_on_stop)
            stop_thread.start()

            # Wait (bounded) until the pending future has been cancelled
            deadline = time.time() + 5.0
            while not result.f.cancelled() and time.time() < deadline:
                time.sleep(0.01)

            if not result.f.cancelled():
                pytest.fail("Pending futures should be cancelled on stop")

            # Once cancellation is observed, release the blocking thread so on_stop can finish
            shutdown.set()
            if stop_thread is not None:
                stop_thread.join(timeout=5.0)
        finally:
            # Release the blocking thread and ensure pool is stopped
            # even if the test fails, preventing thread leaks.
            # on_stop() is idempotent — safe to call twice.
            shutdown.set()
            if stop_thread is not None and stop_thread.is_alive():
                stop_thread.join(timeout=5.0)
            x.on_stop()

    def test_on_stop_fires_hub_timers_while_draining_long_task(self):
        import threading

        from celery.concurrency import thread

        x = thread.TaskPool(limit=1)

        started = threading.Event()
        release = threading.Event()

        def long_task():
            started.set()
            release.wait(timeout=10)

        hub = Mock(name='hub')
        stop_thread = None
        try:
            x.on_apply(long_task, (), {}, noop, noop)
            assert started.wait(timeout=5), "Timed out waiting for long_task to start"

            with patch('celery.concurrency.base.get_event_loop',
                       return_value=hub):
                # Drain (waiting for the long task) in a background thread;
                # heartbeat timers must keep firing while it is blocked.
                stop_thread = threading.Thread(target=x.on_stop)
                stop_thread.start()

                deadline = time.time() + 5.0
                while not hub.fire_timers.called and time.time() < deadline:
                    time.sleep(0.01)
                assert hub.fire_timers.called, \
                    "Hub timers were not fired while the pool was draining"

                release.set()
                stop_thread.join(timeout=5.0)
            assert not stop_thread.is_alive()
        finally:
            release.set()
            if stop_thread is not None and stop_thread.is_alive():
                stop_thread.join(timeout=5.0)
            x.on_stop()

    def test_on_stop_without_hub(self):
        from celery.concurrency import thread

        x = thread.TaskPool()
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=None):
            x.on_stop()
        with pytest.raises(RuntimeError):
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
