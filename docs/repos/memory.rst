
In-Memory Repository
====================

In-memory repository is a data store that is simply 
a Python list in temporary memory.

.. code-block:: python

    from redbird.repos import MemoryRepo
    repo = MemoryRepo()

Usage
-----

Now you may use the repository the same
way as any other repository. Please see:

- :ref:`Reading the repository <read>`
- :ref:`Creating an item to the repository <create>`
- :ref:`Deleting an item from the repository <delete>`
- :ref:`Updating an item in the repository <update>`


Class
-----

.. autoclass:: redbird.repos.MemoryRepo
    :members: insert, filter_by, update, delete