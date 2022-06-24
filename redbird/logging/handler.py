
from copy import copy
from logging import Handler
import logging
from redbird.base import BaseRepo

class RepoHandler(Handler):
    """Log handler that writes log records to a repository

    Useful for cases where the log records need to be read
    programmatically.

    Parameters
    ----------
    repo : BaseRepo
        Repository where the log records are written
    **kwargs : dict
        Keyword arguments passed to logging.Handler
        init
    """

    def __init__(self, repo:BaseRepo, **kwargs):
        self.repo = repo
        super().__init__(**kwargs)

    def emit(self, record:logging.LogRecord):
        "Log the log record"
        record = copy(record)
        msg = self.format(record)
        if isinstance(msg, (dict, logging.LogRecord)):
            # Formatting returned the log record with formatted message
            record = msg
        else:
            # Formatting returned the message as string
            record.formatted_message = msg

        self.write(vars(record))

    def write(self, record:dict):
        "Write a log record to the repository"
        self.repo.add(record)

