SQL Tools
=========

Red Bird's SQL tools are small wrappers to SQLAlchemy's
features to make basic database operations intuitive.
Even though repository patterns are the main focus of 
Red Bird, these tools are fully supported.

``sqlalchemy.Table`` is lower level than needed in simple 
create, read, update and delete operations (know as CRUD).
The user is responsible of properly reflecting the table 
and executing SQL operations requires engine interaction
from the user. Red Bird's :py:class:`redbird.sql.Table`
wraps the table with the engine so that they always point
to a specific table in a specific SQL database. It also
has methods to execute SQL operations directly to the table. 

Here is an example:

.. code-block:: python

    from sqlalchemy import create_engine
    from redbird.sql import Table

    table = Table(name="mytable", bind=create_engine("sqlite://"))

    # Create the table as it does not yet exists
    table.create({"player_name": str, "score": int})

    # Insert rows to the table
    table.insert([
        {"player_name": "Player 1", "score": 100},
        {"player_name": "Player 2", "score": 200}
    ])

    # Read the rows
    table.select({"player_name": "Player 1"})

    # Update a row
    table.update(
        where={"player_name": "Player 1"}, 
        values={"score": 150}
    )

    # Delete a row
    table.delete({"player_name": "Player 2"})

    # Drop (delete) the table
    table.drop()


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   table
   functions
   cookbook
   transaction