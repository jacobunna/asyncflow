"""AsyncFlow executes functions concurrently while respecting
dependencies between them.

AsyncFlow generates "flows" which can be executed using asyncio_,
trio_ or curio_.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _trio: https://trio.readthedocs.io/en/stable/
.. _curio: https://curio.readthedocs.io/en/latest/

Suppose you have four functions, ``setup()``, ``work_1()``,
``work_2()`` and ``shutdown()``. ``setup()`` must be executed first,
but ``work_1()`` and ``work_2()`` can be executed concurrently.
``shutdown()`` can only be executed once all other functions have
finished running. The following code achieves this using asyncio::

    from asyncflow import AsyncioFlow
    import asyncio

    flow = AsyncioFlow()

    @flow()
    def setup(): ...

    @flow(upstream=setup)
    def work_1(): ...

    @flow(upstream=setup)
    def work_2(): ...

    @flow(upstream=[work_1, work_2])
    def shutdown(): ...

    asyncio.run(flow.execute())

AsyncFlow will:

#. Execute ``setup()`` and wait for it to finish;
#. Execute ``work_1()`` and ``work_2()`` concurrently and wait for both
   to finish;
#. Execute ``shutdown()`` and wait for it to finish.
"""

from __future__ import annotations

import abc
import asyncio
import collections
import importlib.util
import types
import warnings
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

if importlib.util.find_spec("curio"):
    import curio
else:
    curio = None
if importlib.util.find_spec("trio"):
    import trio
else:
    trio = None

__all__ = [
    "AsyncioFlow",
    "CurioFlow",
    "TrioFlow",
    "Semaphore",
    "Lock",
    "Sequence",
    "Parallel",
    "WithLock",
]


# Helpers


class Sequence:
    def __init__(self, *args: Union[Sequence, Parallel, WithLock, Job]) -> None:
        self.args = args


class Parallel:
    def __init__(self, *args: Union[Sequence, Parallel, WithLock, Job]) -> None:
        self.args = args


class WithLock(NamedTuple):
    func: Job
    lock: LockPlaceholder


class Lock:
    pass


class Semaphore:
    def __init__(self, value: int) -> None:
        self._value = value


class JobInfo(NamedTuple):
    """Information about a function to be run by asyncflow. """

    upstream: UpstreamType
    lock: LockPlaceholder


class UpstreamDownstream(NamedTuple):
    upstream: DependencyType
    downstream: DependencyType


class InstantiationResult(NamedTuple):
    """Information about whether a class can be instantiated or not. """

    result: bool
    error_message: Optional[str]


# Type definitions

AsyncFunc = Callable[..., Awaitable]
Job = Callable[[], Any]
UpstreamType = Union[List[Job], Job, None]
LockPlaceholder = Union[Lock, Semaphore, None]
DependencyType = Dict[Job, Set[Job]]

RetType = TypeVar("RetType", covariant=True)


class SupportsAsyncWith(Protocol[RetType]):
    async def __aenter__(self) -> RetType:
        pass

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[types.TracebackType],
    ) -> None:
        pass


class LockLike(SupportsAsyncWith[None], Protocol):
    async def acquire(self) -> bool:
        pass

    def release(self) -> None:
        pass


class TaskManagerType(SupportsAsyncWith["TaskManagerType"], Protocol):
    async def run_soon(self, func: AsyncFunc, *args: Any, name: str) -> None:
        pass


# Main ``Flow`` class


class BaseFlow(abc.ABC):
    """Base class for a flow. """

    _jobs: Dict[Job, JobInfo]
    _locks: Dict[LockPlaceholder, Optional[LockLike]]

    @abc.abstractmethod
    def _validate(self) -> InstantiationResult:
        """Ascertain whether this class can be instantiated.

        :return: result of the valiation
        """

    @abc.abstractmethod
    def _get_task_manager(self) -> TaskManagerType:
        """Get a task manager that adheres to the
        :class:`TaskManagerType` protocol.

        :return: the task manager
        """

    @abc.abstractmethod
    def _lock_factory(self) -> LockLike:
        """Get a lock.

        :return: the lock
        """

    @abc.abstractmethod
    def _semaphore_factory(self, value: int) -> LockLike:
        """Get a semaphore.

        :param value: the value of the semaphore i.e. the number of
            parties that can hold the semaphore at one time.
        :return: the semaphore
        """

    @abc.abstractmethod
    async def _run_in_executor(self, func: Job) -> None:
        """Run ``func`` in a thread and await completion.

        :param func: the function to run
        """

    @staticmethod
    def _iscoroutinefunction(func: Job) -> bool:
        """Ascertain whether ``func`` is a coroutine function or a
        synchronous function.

        This is overridden by :class:`CurioFlow` since curio provides
        its own implementation of ``asyncio.iscoroutinefunction``. For
        asyncio and trio this is not overridden.

        :return: ``True`` if ``func`` is a coroutine function or
            ``False`` if ``func`` is a synchronous function.
        """

        return cast(bool, asyncio.iscoroutinefunction(func))

    def __init__(self, spec: Union[Sequence, Parallel, None] = None) -> None:
        """Create a flow.

        :param spec: optionally specify a specification using a
            combination of :class:`Sequence` and :class:`Parallel`
            instances.
        """

        validation_result = self._validate()
        if not validation_result.result:
            raise RuntimeError(
                f"Could not instantiate {self.__class__.__name__}: "
                f"{validation_result.error_message}"
            )
        self._jobs = {}
        self._locks = {}
        if spec:
            self._from_sp_spec(spec, previous=None)

    def _from_sp_spec(
        self, spec: Union[Sequence, Parallel], previous: UpstreamType,
    ) -> UpstreamType:

        if isinstance(spec, Sequence):
            return self._from_sequence(spec, previous)
        if isinstance(spec, Parallel):
            return self._from_parallel(spec, previous)
        raise TypeError(
            f"Invalid type for sequence/parallel specification: {type(spec)}"
        )

    def _from_sequence(self, spec: Sequence, previous: UpstreamType) -> UpstreamType:
        last = previous
        for item in spec.args:
            if isinstance(item, (Sequence, Parallel)):
                last = self._from_sp_spec(item, last)
            elif isinstance(item, WithLock):
                self._add_job(item.func, upstream=last, lock=item.lock)
                last = item.func
            else:
                self._add_job(item, upstream=last, lock=None)
                last = item
        return last

    def _from_parallel(self, spec: Parallel, previous: UpstreamType) -> UpstreamType:
        ret: List[Job] = []
        for item in spec.args:
            if isinstance(item, (Sequence, Parallel)):
                to_append = self._from_sp_spec(item, previous)
                if isinstance(to_append, list):
                    ret.extend(to_append)
                elif to_append is not None:
                    ret.append(to_append)
            elif isinstance(item, WithLock):
                self._add_job(item.func, upstream=previous, lock=item.lock)
                ret.append(item.func)
            else:
                self._add_job(item, upstream=previous, lock=None)
                ret.append(item)
        return ret

    def __call__(
        self, upstream: UpstreamType = None, lock: LockPlaceholder = None
    ) -> Callable[[Job], Job]:
        def decorator(func: Job) -> Job:
            self._add_job(func, upstream=upstream, lock=lock)
            return func

        return decorator

    def _add_job(
        self, func: Job, *, upstream: UpstreamType, lock: LockPlaceholder
    ) -> None:
        if func in self._jobs:
            raise RuntimeError(
                f"Function {func} has already been supplied to asyncflow."
            )
        self._jobs[func] = JobInfo(upstream=upstream, lock=lock)

    async def execute(self) -> None:
        upstream, downstream = self._make_upstream_downstream()
        async with self._get_task_manager() as task_manager:
            for func in self._jobs:
                if not upstream[func]:
                    await self._create_function_task(
                        func, upstream, downstream, task_manager
                    )
        self._warn_about_uncompleted(upstream)

    def _warn_about_uncompleted(self, upstream: DependencyType) -> None:
        for func in self._jobs:
            if upstream[func]:
                formatted_upstreams = ", ".join(
                    f"`{upstream}`" for upstream in upstream[func]
                )
                warnings.warn(
                    f"Job `{func}` did not execute since "
                    f"upstream job(s) {formatted_upstreams} did not complete",
                    RuntimeWarning,
                )

    def _make_upstream_downstream(self) -> UpstreamDownstream:
        upstream: DependencyType = {}
        downstream: DependencyType = collections.defaultdict(set)
        for func, func_data in self._jobs.items():
            if func_data.upstream is None:
                upstream[func] = set()
            elif isinstance(func_data.upstream, list):
                upstream[func] = set(func_data.upstream)
                for upstream_func in func_data.upstream:
                    downstream[upstream_func].add(func)
            else:
                upstream[func] = {func_data.upstream}
                downstream[func_data.upstream].add(func)
        return UpstreamDownstream(upstream, downstream)

    async def _create_function_task(
        self,
        func: Job,
        upstream: DependencyType,
        downstream: DependencyType,
        task_manager: TaskManagerType,
    ) -> None:
        lock = self._get_lock(func)
        await task_manager.run_soon(
            self._execute_function,
            func,
            lock,
            upstream,
            downstream,
            task_manager,
            name=func.__name__,
        )

    def _get_lock(self, func: Job) -> Optional[LockLike]:
        lock_placeholder = self._jobs[func].lock
        if lock_placeholder in self._locks:
            return self._locks[lock_placeholder]
        lock: Optional[LockLike]
        if lock_placeholder is None:
            lock = None
        elif isinstance(lock_placeholder, Lock):
            lock = self._lock_factory()
        elif isinstance(lock_placeholder, Semaphore):
            lock = self._semaphore_factory(lock_placeholder._value)
        else:
            raise TypeError(f"Invalid lock type: {type(lock_placeholder)}")
        self._locks[lock_placeholder] = lock
        return lock

    async def _execute_function(
        self,
        func: Job,
        lock: Optional[LockLike],
        upstream: DependencyType,
        downstream: DependencyType,
        task_manager: TaskManagerType,
    ) -> None:
        if lock is not None:
            async with lock:
                await self._choose_executor(func)
        else:
            await self._choose_executor(func)

        for downstream_func in downstream[func]:
            upstream[downstream_func].remove(func)
            if not upstream[downstream_func]:
                await self._create_function_task(
                    downstream_func, upstream, downstream, task_manager
                )

    async def _choose_executor(self, func: Job) -> None:
        if self._iscoroutinefunction(func):
            await func()
        else:
            await self._run_in_executor(func)


class AsyncioFlow(BaseFlow):
    def _validate(self) -> InstantiationResult:
        # asyncio is part of the standard library, so it is always available
        return InstantiationResult(True, None)

    def _get_task_manager(self) -> TaskManagerType:
        class TaskManager:

            _tasks: List[asyncio.Task]

            def __init__(self) -> None:
                self._tasks = []

            async def __aenter__(self) -> TaskManagerType:
                return self

            async def __aexit__(
                self,
                exc_type: Optional[Type[BaseException]],
                exc: Optional[BaseException],
                tb: Optional[types.TracebackType],
            ) -> None:
                for task in self._tasks:
                    await task

            async def run_soon(self, func: AsyncFunc, *args: Any, name: str) -> None:
                coro = func(*args)
                self._tasks.append(asyncio.create_task(coro, name=name))

        return TaskManager()

    def _lock_factory(self) -> LockLike:
        return cast(LockLike, asyncio.Lock())

    def _semaphore_factory(self, value: int) -> LockLike:
        return cast(LockLike, asyncio.Semaphore(value))

    async def _run_in_executor(self, func: Job) -> None:
        await asyncio.get_running_loop().run_in_executor(None, func)


class CurioFlow(BaseFlow):
    def _validate(self) -> InstantiationResult:
        if curio is None:
            return InstantiationResult(False, "module `curio` could not be imported")
        return InstantiationResult(True, None)

    def _get_task_manager(self) -> TaskManagerType:
        class ModifiedTaskGroup(curio.TaskGroup):
            async def run_soon(self, func: AsyncFunc, *args: Any, name: str) -> None:
                await self.spawn(func, *args)

        out = ModifiedTaskGroup()
        return cast(TaskManagerType, out)

    def _lock_factory(self) -> LockLike:
        return cast(LockLike, curio.Lock())

    def _semaphore_factory(self, value: int) -> LockLike:
        return cast(LockLike, curio.Semaphore(value))

    @staticmethod
    def _iscoroutinefunction(func: Job) -> bool:
        return cast(bool, curio.meta.iscoroutinefunction(func))

    async def _run_in_executor(self, func: Job) -> None:
        t = await curio.spawn_thread(func)
        await t.wait()


class TrioFlow(BaseFlow):
    def _validate(self) -> InstantiationResult:
        if trio is None:
            return InstantiationResult(False, "module `trio` could not be imported")
        return InstantiationResult(True, None)

    def _get_task_manager(self) -> TaskManagerType:
        class TaskManager:
            _nursery: trio.Nursery
            _cm: trio.Nursery

            def __init__(self) -> None:
                self._nursery = trio.open_nursery()
                self._cm = None

            async def __aenter__(self) -> TaskManagerType:
                self._cm = await self._nursery.__aenter__()
                return self

            async def __aexit__(
                self,
                exc_type: Optional[Type[BaseException]],
                exc: Optional[BaseException],
                tb: Optional[types.TracebackType],
            ) -> None:
                await self._nursery.__aexit__(exc_type, exc, tb)

            async def run_soon(self, func: AsyncFunc, *args: Any, name: str) -> None:
                self._cm.start_soon(func, *args)

        return TaskManager()

    def _lock_factory(self) -> LockLike:
        return cast(LockLike, trio.Lock())

    def _semaphore_factory(self, value: int) -> LockLike:
        return cast(LockLike, trio.Semaphore(value))

    async def _run_in_executor(self, func: Job) -> None:
        await trio.to_thread.run_sync(func)
