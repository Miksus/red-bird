
SQL Repository
==============

First, create an ORM item:

.. code-block:: python

    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, Integer, String

    BaseModel = declarative_base()

    class PersonORM(BaseModel):
        id = Column(String, primary=True)
        name = Column(String)
        age = Column(Integer)

Initiate a repository:

.. code-block:: python

    from redbird.ext import SQLRepo
    from sqlalchemy import create_engine

    repo = SQLRepo(model_orm=PersonORM, engine=create_engine('sqlite://'))

Pydantic
--------

You may also create a Pydantic model on top so that the returned 
item is more alligned with other repositories:

.. code-block:: python

    from pydantic import BaseModel

    class Person(BaseModel):
        id: str
        name: str
        age: int

        # We also need to set ORM mode as True
        class Config:
            orm_mode = True

Initiate a repository:

.. code-block:: python

    from redbird.ext import SQLRepo
    from sqlalchemy import create_engine

    repo = SQLRepo(Person, model_orm=PersonORM, engine=create_engine('sqlite://'))



Now you may use the repository the same
way as any other repository. Please see:

- :ref:`Reading the repository <read>`
- :ref:`Creating an item to the repository <create>`
- :ref:`Deleting an item from the repository <delete>`
- :ref:`Updating an item in the repository <update>`