Table
=====

:py:class:`redbird.sql.Table` is further abstraction from
``sqlalchemy.Table``. It combines the connection with the 
table itself thus it is always pointing to an actual SQL
table (which may not yet exist). It also makes certain
operations more intuitive and let you leverage Python's
native data types more than SQLAlchemy does.

Class
-----

.. autoclass:: redbird.sql.Table
    :members: select, insert, update, delete, create, drop, exists, execute, count, transaction, open_transaction