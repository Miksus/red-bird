
SQL ORM Repository
==================

SQLRepo is an SQL repository that relies on 
SQLAlchemy's object-relational mapping (ORM).
The difference to ``SQLExprRepo`` is that 
``SQLRepo`` is more object centric and it may
be useful if you already use ORM in some places.
It is also more complex and in some cases more 
error prone.


SQLRepo can be initiated numerous ways. You may 
initiate it with session, engine or SQLAlchemy 
connection string and you may optionally supply
ORM model.

.. code-block:: python

    from sqlalchemy import create_engine
    from redbird.repos import SQLRepo

    engine = create_engine('sqlite://')
    repo = SQLRepo(engine=engine, table="my_table")

You may also supply a session:

.. code-block:: python

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from redbird.repos import SQLRepo

    engine = create_engine('sqlite://')
    session = Session(engine)
    repo = SQLRepo(session=session, table="my_table")

Using ORM model:

.. code-block:: python

    from sqlalchemy.orm import declarative_base
    
    Base = declarative_base()

    class Car(Base):
        __tablename__ = 'my_table'
        color = Column(String, primary_key=True)
        car_type = Column(String)
        milage = Column(Integer)

    from sqlalchemy import create_engine
    from redbird.repos import SQLRepo

    engine = create_engine('sqlite://')
    repo = SQLRepo(orm=Car, engine=engine)

Using ORM model and reflect Pydantic Model:

.. code-block:: python

    from sqlalchemy.orm import declarative_base
    
    Base = declarative_base()

    class Car(Base):
        __tablename__ = 'my_table'
        color = Column(String, primary_key=True)
        car_type = Column(String)
        milage = Column(Integer)

    from sqlalchemy import create_engine
    from redbird.repos import SQLRepo

    engine = create_engine('sqlite://')
    repo = SQLRepo(orm=Car, reflect_model=True, engine=engine)

Using ORM model and Pydantic Model:

.. code-block:: python

    from pydantic import BaseModel
    from sqlalchemy.orm import declarative_base
    
    Base = declarative_base()

    class CarORM(Base):
        __tablename__ = 'my_table'
        color = Column(String, primary_key=True)
        car_type = Column(String)
        milage = Column(Integer)

    class Car(BaseModel):
        id: str
        name: str
        age: int

    from sqlalchemy import create_engine
    from redbird.repos import SQLRepo

    engine = create_engine('sqlite://')
    repo = SQLRepo(model=Car, orm=CarORM, engine=engine)

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

.. autoclass:: redbird.repos.SQLRepo
    :members: insert, filter_by, update, delete