"""Tests for asyncflow. """

# mypy: ignore-errors
# pylint: disable=R0201,W0613,C0111,W0612

import asyncio
import time

import curio
import pytest
import trio

import asyncflow
from asyncflow import (
    AsyncioFlow,
    CurioFlow,
    Lock,
    Parallel,
    Semaphore,
    Sequence,
    TrioFlow,
    WithLock,
)


def run_asyncio(func):
    """Wrapper for ``asyncio.run``.

    Whereas ``curio.run`` and ``trio.run`` take an async function as
    their argument, ``asyncio.run`` takes a coroutine. To create a
    uniform interface this wrapper is used instead.
    """

    asyncio.run(func())


@pytest.mark.parametrize(
    ["flow_cls", "runner", "sleep"],
    [
        (AsyncioFlow, run_asyncio, asyncio.sleep),
        (CurioFlow, curio.run, curio.sleep),
        (TrioFlow, trio.run, trio.sleep),
    ],
)
class Tests:
    def test_asyncioflow(self, flow_cls, runner, sleep):
        """Basic test case. """

        calls = []
        flow = flow_cls()

        @flow()
        async def f():
            calls.append("f")

        @flow(upstream=f)
        async def g():
            calls.append("g")

        @flow(upstream=g)
        async def h():
            calls.append("h")

        runner(flow.execute)
        assert calls == ["f", "g", "h"]

    def test_asyncioflow_multiple_paths(self, flow_cls, runner, sleep):
        """Test asyncioflow when there is more than one valid execution
        order.
        """

        calls = []
        flow = flow_cls()

        @flow()
        def f():
            calls.append("f")

        @flow(upstream=f)
        def g():
            calls.append("g")

        @flow(upstream=f)
        def h():
            calls.append("h")

        @flow(upstream=[g, h])
        def i():
            calls.append("i")

        runner(flow.execute)
        assert calls[0] == "f"
        assert set(calls[1:3]) == {"g", "h"}
        assert calls[3] == "i"

    def test_asyncioflow_complex(self, flow_cls, runner, sleep):
        """Test a more complex looking DAG of executions. """

        calls = []
        flow = flow_cls()

        @flow()
        async def f():
            calls.append("f")

        @flow()
        async def g():
            calls.append("g")

        @flow(upstream=f)
        async def h():
            calls.append("h")

        @flow(upstream=[g, h])
        async def i():
            calls.append("i")

        @flow(upstream=[g, h])
        async def j():
            calls.append("j")

        @flow(upstream=[g, h])
        async def k():
            calls.append("k")

        @flow(upstream=[i, j, k])
        async def m():
            calls.append("m")

        runner(flow.execute)
        assert calls[0:3] in (["f", "g", "h"], ["f", "h", "g"], ["g", "f", "h"])
        assert set(calls[3:6]) == {"i", "j", "k"}
        assert calls[6] == "m"

    def test_sequence_parallel(self, flow_cls, runner, sleep):
        """Test ``Sequence`` and ``Parallel`` flow specification. """

        calls = []

        async def f():
            calls.append("f")

        async def g():
            calls.append("g")

        async def h():
            calls.append("h")

        async def i():
            calls.append("i")

        async def j():
            calls.append("j")

        async def k():
            calls.append("k")

        async def m():
            calls.append("m")

        flow = flow_cls(Sequence(Parallel(Sequence(f, h), g), Parallel(i, j, k), m))
        runner(flow.execute)
        assert calls[0:3] in (["f", "g", "h"], ["f", "h", "g"], ["g", "f", "h"])
        assert set(calls[3:6]) == {"i", "j", "k"}
        assert calls[6] == "m"

    def test_threads(self, flow_cls, runner, sleep):
        """Test when some tasks are run in threads. """

        calls = []
        flow = flow_cls()

        @flow()
        async def f():
            calls.append("f")

        @flow(upstream=f)
        def g():
            calls.append("g")

        @flow(upstream=g)
        def h():
            calls.append("h")

        runner(flow.execute)
        assert calls == ["f", "g", "h"]

    def test_concurrent(self, flow_cls, runner, sleep):
        """Check sleeping can occur concurrently. """

        calls = []
        flow = flow_cls()
        start = time.time()

        @flow()
        async def f():
            calls.append("f")
            await sleep(0.1)

        @flow()
        async def g():
            calls.append("g")
            await sleep(0.1)

        @flow()
        async def h():
            calls.append("h")
            await sleep(0.1)

        runner(flow.execute)
        assert 0.1 < (time.time() - start) < 0.2

    def test_lock(self, flow_cls, runner, sleep):
        """Test locks. """

        flow = flow_cls()
        start = time.time()
        lock = Lock()

        @flow(lock=lock)
        async def f():
            await sleep(0.1)

        @flow(lock=lock)
        async def g():
            await sleep(0.1)

        @flow(lock=lock)
        async def h():
            await sleep(0.1)

        runner(flow.execute)
        assert (time.time() - start) > 0.3

    def test_semaphore(self, flow_cls, runner, sleep):
        """Test semaphores. """

        flow = flow_cls()
        start = time.time()
        lock = Semaphore(2)

        @flow(lock=lock)
        async def f():
            await sleep(0.1)

        @flow(lock=lock)
        async def g():
            await sleep(0.1)

        @flow(lock=lock)
        async def h():
            await sleep(0.1)

        runner(flow.execute)
        assert 0.2 < (time.time() - start) < 0.3

    def test_withlock(self, flow_cls, runner, sleep):
        """Test with ``WithLock`` class for programatically specifying
        a lock.
        """

        start = time.time()

        async def f():
            await sleep(0.1)

        async def g():
            await sleep(0.1)

        async def h():
            await sleep(0.1)

        lock = Lock()
        flow = flow_cls(
            Parallel(WithLock(f, lock), WithLock(g, lock), WithLock(h, lock))
        )
        runner(flow.execute)
        assert (time.time() - start) > 0.3

    def test_function_did_not_run(self, flow_cls, runner, sleep):
        """Check a warning is raised when a function does not run. """

        flow = flow_cls()

        def f():
            pass

        @flow(upstream=f)
        async def g():
            pass

        with pytest.warns(RuntimeWarning):
            runner(flow.execute)


@pytest.mark.parametrize(
    ["module_name", "flow_cls"], [("curio", CurioFlow), ("trio", TrioFlow)]
)
def test_module_unavailable(module_name, flow_cls, monkeypatch):
    """Test an exception is raised when a required module is not
    available.
    """

    monkeypatch.setattr(asyncflow, module_name, None)
    with pytest.raises(RuntimeError):
        flow_cls()
