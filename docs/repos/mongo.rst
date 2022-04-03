
Mongo Repository
====================

First, create an item:

.. code-block:: python

    from pydantic import BaseModel

    class Person(BaseModel):
        id: str
        name: str
        age: int

Initiate a repository:

.. code-block:: python

    from redbase.ext import MongoRepo

    repo = MongoRepo(Person, url="mongodb://localhost:27017/my_database")


Now you may use the repository the same
way as any other repository. Please see:

- :ref:`Reading the repository <read>`
- :ref:`Creating an item to the repository <create>`
- :ref:`Deleting an item from the repository <delete>`
- :ref:`Updating an item in the repository <update>`