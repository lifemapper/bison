"""Module containing standardized logging class for lmpy."""
import logging
import os
import sys

from obsolete.src.common.constants2 import LOG


# .....................................................................................
class Logger:
    """Class containing a logger for consistent logging."""

    # .......................
    def __init__(
            self, logger_name, log_filename=None, log_console=True, log_level=logging.DEBUG):
        """Constructor.

        Args:
            logger_name (str): A name for the logger.
            log_filename (str): A file location to write logging information.
            log_console (bool): Should logs be written to the console.
            log_level (int): What level of logs should be retained.
        """
        self.logger = None
        self.name = logger_name
        self.filename = log_filename
        self.log_directory = None
        self.log_console = log_console
        self.log_level = log_level

        handlers = []
        if self.filename is not None:
            self.log_directory = os.path.dirname(log_filename)
            os.makedirs(self.log_directory, exist_ok=True)
            handlers.append(logging.FileHandler(log_filename, mode="w"))

        if self.log_console:
            handlers.append(logging.StreamHandler(stream=sys.stdout))

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(LOG.FORMAT, LOG.DATE_FORMAT)
        for handler in handlers:
            handler.setLevel(self.log_level)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.propagate = False

    # ........................
    def log(self, msg, refname="", log_level=logging.INFO):
        """Log a message.

        Args:
            msg (str): A message to write to the logger.
            refname (str): Class or function name to use in logging message.
            log_level (int): A level to use when logging the message.
        """
        if self.logger is not None:
            self.logger.log(log_level, refname + ': ' + msg)


# .....................................................................................
__all__ = ["Logger"]
