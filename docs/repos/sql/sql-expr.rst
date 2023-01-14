
SQL Expressions Repository
==========================

SQLExprRepo is a repository that 
relies on SQLAlchemy's SQL expressions
which are pieces of SQL code represented
in Python.

It can be initiated using SQLAlchemy engine:

.. code-block:: python

    from sqlalchemy import create_engine
    from redbird.repos import SQLExprRepo

    engine = create_engine('sqlite://')
    repo = SQLExprRepo(engine=engine, table="my_table")


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

.. autoclass:: redbird.repos.SQLExprRepo
    :members: insert, filter_by, update, delete