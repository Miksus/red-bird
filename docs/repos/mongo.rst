
Mongo Repository
================

MongoRepo is a repository for MongoDB data stores.
MongoDB is useful if you have unstructured data
or you wish to store data in JSON format.

.. code-block:: python

    from redbird.repos import MongoRepo
    repo = MongoRepo(uri="mongodb://USERNAME:PASSWORD@localhost:27017", database="my_db", collection="my_items")

Alternatively, you may pass the client:

.. code-block:: python

    from redbird.repos import MongoRepo
    from pymongo import MongoClient
    repo = MongoRepo(client=MongoClient("mongodb://USERNAME:PASSWORD@localhost:27017"), database="my_db", collection="my_items")

With Pydantic model:

.. code-block:: python

    from redbird.repos import MongoRepo

    class MyItem(BaseModel):
        id: str
        name: str
        age: int

    repo = MongoRepo(
        uri="mongodb://USERNAME:PASSWORD@localhost:27017", 
        database="my_db", 
        collection="my_items",
        model=MyItem
    )

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

.. autoclass:: redbird.repos.MongoRepo
    :members: insert, filter_by, update, delete