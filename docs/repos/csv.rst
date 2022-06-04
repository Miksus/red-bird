
CSV Repository
==============

CSV (Comma-separated values file) repository is a data store
in which each item represents a row in the given CSV file.

With dict:

.. code-block:: python

    from redbird.repos import CSVFileRepo
    repo = CSVFileRepo(filename="my_repo.csv", fieldnames=['id', 'name', 'age'])

.. warning::

    CSV files don't maintain the data types. All field values are considered
    ``str`` and empty values are considered ``None``. It is adviced to use
    Pydantic model (see below) instead if the type matters.

With Pydantic model:

.. code-block:: python

    from pydantic import BaseModel
    from redbird.repos import CSVFileRepo

    class MyItem(BaseModel):
        id: str
        name: str
        age: int

    repo = CSVFileRepo(filename="my_repo.csv", model=MyItem)


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

.. autoclass:: redbird.repos.CSVFileRepo
    :members: insert, filter_by, update, delete