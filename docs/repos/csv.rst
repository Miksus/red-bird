
CSV Repository
==============

CSV (Comma-separated values file) repository is a data store
in which each item represents a row in the given CSV file.

.. code-block:: python

    from redbird.repos import CSVFileRepo
    repo = CSVFileRepo(filename="my_repo.csv")

Usage
-----

Now you may use the repository the same
way as any other repository. Please see:

- :ref:`Reading the repository <read>`
- :ref:`Creating an item to the repository <create>`
- :ref:`Deleting an item from the repository <delete>`
- :ref:`Updating an item in the repository <update>`
