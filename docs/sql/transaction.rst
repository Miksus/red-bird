Transaction
===========

There are three ways to do SQL transactions 
with :py:class:`redbird.sql.Table`

- Using :py:meth:`redbird.sql.Table.open_transaction`
- Using :py:meth:`redbird.sql.Table.transaction`
- Opening it manually and create the table

The first two are covered in Table's documentation 
but in some cases you might need to open the transaction
manually. 

For example, if the transaction operates on multiple tables:

.. code-block:: python

    from sqlalchemy import create_engine
    from redbird.sql import Table

    engine = create_engine("sqlite://")

    with engine.begin() as trans:

        car_table = Table("car", bind=trans)
        van_table = Table("van", bind=trans)

        # Do operations
        car_table.insert(...)
        van_table.delete(...)
