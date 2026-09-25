import itertools
import time
from unittest.mock import Mock, patch

import pytest
from billiard.einfo import ExceptionInfo

from celery.concurrency.base import firing_hub_timers

pytest.importorskip('multiprocessing')


def wait_for(condition, timeout=5.0):
    """Poll condition() until true or the timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def do_something(i):
    return i * i


def long_something():
    time.sleep(1)


def raise_something(i):
    try:
        raise KeyError('FOO EXCEPTION')
    except KeyError:
        return ExceptionInfo()


class test_TaskPool:

    def setup_method(self):
        from celery.concurrency.prefork import TaskPool
        self.TaskPool = TaskPool

    def test_attrs(self):
        p = self.TaskPool(2)
        assert p.limit == 2
        assert p._pool is None

    def x_apply(self):
        p = self.TaskPool(2)
        p.start()
        scratchpad = {}
        proc_counter = itertools.count()

        def mycallback(ret_value):
            process = next(proc_counter)
            scratchpad[process] = {}
            scratchpad[process]['ret_value'] = ret_value

        myerrback = mycallback

        res = p.apply_async(do_something, args=[10], callback=mycallback)
        res2 = p.apply_async(raise_something, args=[10], errback=myerrback)
        res3 = p.apply_async(do_something, args=[20], callback=mycallback)

        assert res.get() == 100
        time.sleep(0.5)
        assert scratchpad.get(0)['ret_value'] == 100

        assert isinstance(res2.get(), ExceptionInfo)
        assert scratchpad.get(1)
        time.sleep(1)
        assert isinstance(scratchpad[1]['ret_value'], ExceptionInfo)
        assert scratchpad[1]['ret_value'].exception.args == ('FOO EXCEPTION',)

        assert res3.get() == 400
        time.sleep(0.5)
        assert scratchpad.get(2)['ret_value'] == 400

        res3 = p.apply_async(do_something, args=[30], callback=mycallback)

        assert res3.get() == 900
        time.sleep(0.5)
        assert scratchpad.get(3)['ret_value'] == 900
        p.stop()


class test_firing_hub_timers:

    def test_no_hub_is_noop(self):
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=None), \
                patch('celery.concurrency.base.Thread') as mock_thread:
            with firing_hub_timers():
                pass
        mock_thread.assert_not_called()

    def test_fires_timers_while_blocked(self):
        hub = Mock(name='hub')
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=hub):
            with firing_hub_timers(interval=0.01):
                assert wait_for(lambda: hub.fire_timers.call_count >= 2)
        assert hub.fire_timers.call_count >= 2

    def test_stops_firing_after_exit(self):
        hub = Mock(name='hub')
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=hub):
            with firing_hub_timers(interval=0.01):
                assert wait_for(lambda: hub.fire_timers.call_count >= 1)
            # the timer thread is joined on exit, so no more timers fire.
            call_count = hub.fire_timers.call_count
            time.sleep(0.1)
            assert hub.fire_timers.call_count == call_count

    def test_fire_timers_exception_does_not_break_the_loop(self):
        hub = Mock(name='hub')
        hub.fire_timers.side_effect = [Exception('boom'), None, None]
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=hub):
            with firing_hub_timers(interval=0.01):
                assert wait_for(lambda: hub.fire_timers.call_count >= 3)

    def test_keeps_firing_while_long_task_drains(self):
        # Simulates shutdown waiting for a long-running task: the broker
        # heartbeat timers must keep firing for the whole drain.
        hub = Mock(name='hub')
        with patch('celery.concurrency.base.get_event_loop',
                   return_value=hub):
            with firing_hub_timers(interval=0.1):
                time.sleep(0.55)  # long task still running
            assert hub.fire_timers.call_count >= 2
