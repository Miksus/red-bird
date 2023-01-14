
.. _version-history:

Version history
===============

- ``0.7.0``

    - SQL: add new repo, :py:class:`redbird.repos.SQLExprRepo`, which relies on SQLAlchemy' SQL expressions
    - SQL: add SQL tools (``redbird.sql``) to make SQL operations more intuitive

- ``0.6.1``

    - SQL: fix model based ``__id_field__`` in reflection
    - SQL: fix date-like types in table creation with ``if_missing='create'``

- ``0.6.0``

    - Added alternative way to set repository's ID field: setting ``__id_field__`` in model
    - Packaging updated to use pyproject.toml

- ``0.5.1``

    - Fixed ``CSVFileRepo`` error if read and the file does not exists

- ``0.5.0``

    - Added ``in`` operator

- ``0.4.0``

    - Added logging utility: ``RepoHandler``
    - Fixed not committing deletes and updates in SQLRepo
    - Fixed ORM model reflection in SQLRepo
    - Continued documentation

- ``0.3.0``

    - First official release
    - Stabilized the API
    - Updated documentation
    - Added several new repositories

- ``0.2.4``

    - First release