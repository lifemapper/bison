import csv
import logging
from logging.handlers import RotatingFileHandler
import sys
# import unicodecsv

from gbif.constants import (LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                       LOGFILE_BACKUP_COUNT)

# # .............................................................................
# def getCSVReader(datafile, delimiter, encoding):
#     try:
#         f = open(datafile, 'r') 
#         reader = unicodecsv.reader(f, delimiter=delimiter, encoding=encoding)        
#     except Exception as e:
#         raise Exception('Failed to read or open {}, ({})'
#                         .format(datafile, str(e)))
#     return reader, f
# 
# # .............................................................................
# def getCSVWriter(datafile, delimiter, encoding, doAppend=True):
#     '''
#     @summary: Get a CSV writer that can handle encoding
#     '''
#     unicodecsv.field_size_limit(sys.maxsize)
#     if doAppend:
#         mode = 'ab'
#     else:
#         mode = 'wb'
#     try:
#         f = open(datafile, mode) 
#         writer = unicodecsv.writer(f, delimiter=delimiter, 
#                                    encoding=encoding)
#     except Exception as e:
#         raise Exception('Failed to read or open {}, ({})'
#                         .format(datafile, str(e)))
#     return writer, f


# .............................................................................
def getCSVReader(datafile, delimiter, encoding):
    try:
        f = open(datafile, 'r', encoding=encoding) 
        reader = csv.reader(f, delimiter=delimiter)        
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return reader, f

# .............................................................................
def getCSVWriter(datafile, delimiter, encoding, doAppend=True):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    csv.field_size_limit(sys.maxsize)
    if doAppend:
        mode = 'a'
    else:
        mode = 'w'
    try:
        f = open(datafile, mode, encoding=encoding) 
        writer = csv.writer(f, delimiter=delimiter)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return writer, f


# .............................................................................
def getLogger(name, fname):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handlers = []
    handlers.append(RotatingFileHandler(fname, maxBytes=LOGFILE_MAX_BYTES, 
                                        backupCount=LOGFILE_BACKUP_COUNT))
    handlers.append(logging.StreamHandler())
    for fh in handlers:
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        log.addHandler(fh)
    return log

# ...............................................
def getLine(csvreader, recno):
    '''
    @summary: Return a line while keeping track of the line number and errors
    '''
    success = False
    line = None
    while not success and csvreader is not None:
        try:
            line = next(csvreader)
            if line:
                recno += 1
            success = True
        except OverflowError as e:
            recno += 1
            print( 'Overflow on record {}, line {} ({})'
                                 .format(recno, csvreader.line_num, str(e)))
        except StopIteration:
            print('EOF after record {}, line {}'
                                .format(recno, csvreader.line_num))
            success = True
        except Exception as e:
            recno += 1
            print('Bad record on record {}, line {} ({})'
                                .format(recno, csvreader.line_num, e))

    return line, recno

