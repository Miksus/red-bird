
.. _version-history:

Version history
===============

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