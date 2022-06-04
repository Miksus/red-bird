
JSON Directory Repository
=========================

JSON (JavaScript Object Notation) directory repository is a data store
in which each item represents a JSON file in a directory. The stems of
the files are the IDs specified in the ``id_field`` of the repository.

.. code-block:: python

    from redbird.repos import JSONDirectoryRepo
    repo = JSONDirectoryRepo(path="path/to/repo", id_field='id')

With Pydantic model:

.. code-block:: python

    from pydantic import BaseModel
    from redbird.repos import JSONDirectoryRepo

    class MyItem(BaseModel):
        id: str
        name: str
        age: int

    repo = JSONDirectoryRepo(path="path/to/repo", model=MyItem, id_field='id')

.. note::

    The ``id_field`` must be specified as the name of the JSON file 
    depends on the value of this field.

.. warning::

    The order in which the items are stored depends on the operating system
    thus the order in which the items are returned when reading the repository
    is not guaranteed to be fixed.

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

.. autoclass:: redbird.repos.JSONDirectoryRepo
    :members: insert, filter_by, update, delete