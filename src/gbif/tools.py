import logging
from logging.handlers import RotatingFileHandler
import sys
import unicodecsv

from constants import (ENCODING, LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                       LOGFILE_BACKUP_COUNT)

# .............................................................................
def getCSVReader(datafile, delimiter):
   '''
   @summary: Get a CSV reader that can handle encoding
   '''
   f = None  
   unicodecsv.field_size_limit(sys.maxsize)
   try:
      f = open(datafile, 'rb')
      reader = unicodecsv.reader(f, delimiter=delimiter, encoding=ENCODING)
   except Exception, e:
      raise Exception('Failed to read or open {}, ({})'
                      .format(datafile, str(e)))
   return reader, f

# .............................................................................
def getCSVWriter(datafile, delimiter):
   '''
   @summary: Get a CSV writer that can handle encoding
   '''
   unicodecsv.field_size_limit(sys.maxsize)
   try:
      f = open(datafile, 'wb') 
      writer = unicodecsv.writer(f, delimiter=delimiter, encoding=ENCODING)

   except Exception, e:
      raise Exception('Failed to read or open {}, ({})'
                      .format(datafile, str(e)))
   return writer, f


def getLogger(name, fname):
   log = logging.getLogger(name)
   log.setLevel(logging.DEBUG)
   formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
   fileLogHandler = RotatingFileHandler(fname, 
                                        maxBytes=LOGFILE_MAX_BYTES, 
                                        backupCount=LOGFILE_BACKUP_COUNT)
   fileLogHandler.setLevel(logging.DEBUG)
   fileLogHandler.setFormatter(formatter)
   log.addHandler(fileLogHandler)
   return log
