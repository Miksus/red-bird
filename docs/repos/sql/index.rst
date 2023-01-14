SQL Repositories
================

There are two SQL related repositories in Red Bird:

- :py:class:`redbird.repos.SQLRepo`: Built around object-relational mapping
- :py:class:`redbird.repos.SQLExprRepo`: Built around SQL expressions

Both of them relies on functionalities provided by 
SQLAlchemy. In most cases :py:class:`redbird.repos.SQLExprRepo` is right 
choice as often it is less error prone as it is simpler. 
:py:class:`redbird.repos.SQLRepo` is useful if you need to 
leverage more from SQLAlchemy's ORM features.

They also differ in terms of how they do the connection:

- :py:class:`redbird.repos.SQLRepo`: Relies on ``sqlalchemy.orm.Session``
- :py:class:`redbird.repos.SQLExprRepo`: Relies on ``sqlalchemy.engine.Engine``

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   sql-expr
   sql-orm