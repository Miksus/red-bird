Cookbook
========

Using Native Python
-------------------

This is an example to operate on :py:meth:`redbird.sql.Table`
using native Python data types:

.. code-block:: python

    from redbird.sql import Table
    from sqlalchemy import create_engine

    engine = create_engine("sqlite://")
    table = Table("shopping_list", bind=engine)

    # Create a table
    table.create({"product": str, "quantity": int})

    # Insert one
    table.insert({"product": "milk", "quantity": 2})

    # Insert multiple
    table.insert([
        {"product": "juice", "quantity": 1},
        {"product": "eggs", "quantity": 6},
    ])

    # Select rows where product is milk and quantity is 1
    list(table.select({"product": "milk", "quantity": 1}))

    # Update item(s)
    table.update(where={"product": "milk"}, values={"quantity": 3})

    # Delete item(s)
    table.delete({"product": "juice"})

Using SQL Expressions
---------------------

This is an example to operate on :py:meth:`redbird.sql.Table`
using SQLAlchemy's SQL expressions:

.. code-block:: python

    from redbird.sql import Table
    from sqlalchemy import create_engine
    import sqlalchemy

    engine = create_engine("sqlite://")
    table = Table("shopping_list", bind=engine)

    # Create a table
    table.create([
        sqlalchemy.Column("product", type_=sqlalchemy.String(), primary_key=True),
        sqlalchemy.Column("quantity", type_=sqlalchemy.Integer(), primary_key=False),
    ])

    # Insert multiple
    table.insert([
        {"product": "milk", "quantity": 2},
        {"product": "juice", "quantity": 1},
        {"product": "eggs", "quantity": 6},
    ])

    # Select rows where product is milk and quantity is 1
    qry = (sqlalchemy.Column("product") == "milk") & (sqlalchemy.Column("quantity") == 1)
    list(table.select(qry))

    # Update item(s)
    table.update(where=sqlalchemy.Column("product") == "milk", values={"quantity": 3})

    # Delete item(s)
    table.delete(sqlalchemy.Column("product") == "juice")

Operating on an Item
--------------------

First we create an example table with some 
example data:

.. code-block:: python

    from sqlalchemy import create_engine
    from redbird.sql import Table

    table = Table(name="mytable", bind=create_engine("sqlite://"))

    # Create a table that has index/indices
    table.create([
        {"name": "index_1", "type_": str, "primary_key": True},
        {"name": "index_2", "type_": int, "primary_key": True},
        {"name": "firstname", "type_": str, "primary_key": False},
    ])

    # Create data
    table.insert([
        {"index_1": "a", "index_2": 1, "firstname": "Jack"},
        {"index_1": "a", "index_2": 2, "firstname": "John"},
        {"index_1": "b", "index_2": 1, "firstname": "James"},
        {"index_1": "b", "index_2": 2, "firstname": "Johnny"},
        {"index_1": "c", "index_2": 1, "firstname": "Jimmy"},
        {"index_1": "c", "index_2": 2, "firstname": "Jim"},
    ])

Selecting a specific item:

.. code-block:: python

    table[("a", 1)]

.. note::

    This is same as:

    .. code-block:: sql

        SELECT *
        FROM mytable
        WHERE index_1 = 'a' AND index_2 = 1

You can also select range of items:

- ``table["a"]``: Get all where ``index_1`` is ``"a"``
- ``table["a":"b"]``: Get all where ``index_1`` from ``"a"`` to ``"b"``
- ``table[["a", "c"]]``: Get all where ``index_1`` is ``"a"`` or ``"c"``

Similarly, you can also delete item(s):

.. code-block:: python

    del table[("a", 1)]

.. note::

    This is same as:

    .. code-block:: sql

        DELETE FROM mytable
        WHERE index_1 = 'a' AND index_2 = 1