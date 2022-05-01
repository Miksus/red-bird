
Mongo Repository
================

MongoRepo is a repository for MongoDB data stores.
MongoDB is useful if you have unstructured data
or you wish to store data in JSON format.

.. code-block:: python

    from redbird.repos import MongoRepo
    repo = MongoRepo(url="mongodb://USERNAME:PASSWORD@localhost:27017", database="my_db", collection="my_items")

Usage
-----

Now you may use the repository the same
way as any other repository. Please see:

- :ref:`Reading the repository <read>`
- :ref:`Creating an item to the repository <create>`
- :ref:`Deleting an item from the repository <delete>`
- :ref:`Updating an item in the repository <update>`
