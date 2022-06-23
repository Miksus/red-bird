
.. _logging_handler:

Logging Handler
===============

Red Bird also has a logging handler which is 
useful if you need to store the log records 
in a database or you wish to read the records
later programmatically.

.. code-block:: python

    import logging
    from redbird.logging import RepoHandler
    from redbird.repos import MemoryRepo

    # Create a repo 
    log_repo = MemoryRepo()

    # Create a handler
    handler = RepoHandler(repo=log_repo)

    # Set the handler to a logger
    logger = logging.getLogger('mylogger')
    logger.addHandler(handler)

.. note::

    In this example we used a repository
    that logs the records to an in-memory
    list. Read more about supported repositories
    here: :ref:`repositories`

Then to use it:

.. code-block:: python

    logger.debug("A debug message")
    logger.info("An info message")
    logger.warning("A warning message")

To read the log records:

.. code-block:: python

    log_repo.filter_by(levelname="INFO").all()

.. note::

    Read more about log record attributes: 
    `logging.LogRecord <https://docs.python.org/3/library/logging.html#logrecord-attributes>`_.
    
    ``RepoHandler`` adds one extra attribute,
    ``formatted_message``, that represents
    the message after it has been processed
    by the handler's formatter.
    

With a Record Model
-------------------

If you need customization on the log record that is inserted
to the database, you may create a Pydantic model and set that
as the model of the repository. Here is an example to do so: 

.. code-block:: python

    from pydantic import BaseModel, Field

    class LogRecord(BaseModel):
        """A logging record

        See attributes: https://docs.python.org/3/library/logging.html#logrecord-attributes
        """
        name: str
        msg: str
        levelname: str
        levelno: int
        pathname: str
        filename: str
        module: str
        exc_info: str
        exc_text: str
        stack_info: str
        lineno: int
        funcName: str
        created: float
        msecs: float
        relativeCreated: float
        thread: int
        threadName: str
        processName: str
        process: int
        message: str

        formatted_message: str = Field(description="Formatted message. This field is created by RepoHandler.")

Then to set the model:

.. code-block:: python

    import logging
    from redbird.logging import RepoHandler
    from redbird.repos import MemoryRepo

    log_repo = MemoryRepo(model=LogRecord)
    handler = RepoHandler(repo=log_repo)

    logger = logging.getLogger('mylogger')
    logger.addHandler(handler)
