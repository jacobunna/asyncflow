API Reference
=============

This page documents the public API exposed by AsyncFlow.

.. toctree::

Creating a Flow
---------------

.. autoclass:: asyncflow.BaseFlow
    :members: __call__
.. autoclass:: asyncflow.AsyncioFlow
.. autoclass:: asyncflow.CurioFlow
.. autoclass:: asyncflow.TrioFlow

Locks
-----

.. autoclass:: asyncflow.Lock
.. autoclass:: asyncflow.Semaphore

Alternative API
---------------

.. autoclass:: asyncflow.Sequence
.. autoclass:: asyncflow.Parallel
.. autoclass:: asyncflow.WithLock
