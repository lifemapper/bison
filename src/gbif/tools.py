import logging
from logging.handlers import RotatingFileHandler
import sys
import unicodecsv

from constants import (LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                       LOGFILE_BACKUP_COUNT)

# # .............................................................................
# def getCSVReader(datafile, delimiter):
#     '''
#     @summary: Get a CSV reader that can handle encoding
#     '''
#     f = None  
#     unicodecsv.field_size_limit(sys.maxsize)
#     try:
#         f = open(datafile, 'rb')
#         reader = unicodecsv.reader(f, delimiter=delimiter, encoding=ENCODING)
#     except Exception, e:
#         raise Exception('Failed to read or open {}, ({})'
#                              .format(datafile, str(e)))
#     return reader, f
# 
# # .............................................................................
# def getCSVWriter(datafile, delimiter, doAppend=True):
#     '''
#     @summary: Get a CSV writer that can handle encoding
#     '''
#     unicodecsv.field_size_limit(sys.maxsize)
#     if doAppend:
#         mode = 'ab'
#     else:
#         mode = 'wb'
#         
#     try:
#         f = open(datafile, mode) 
#         writer = unicodecsv.writer(f, delimiter=delimiter, encoding=ENCODING)
# 
#     except Exception, e:
#         raise Exception('Failed to read or open {}, ({})'
#                              .format(datafile, str(e)))
#     return writer, f

# .............................................................................
def getCSVReader(datafile, delimiter, encoding):
    try:
        f = open(datafile, 'r') 
        reader = unicodecsv.reader(f, delimiter=delimiter, encoding=encoding)        
    except Exception, e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return reader, f

# .............................................................................
def getCSVWriter(datafile, delimiter, encoding, doAppend=True):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    unicodecsv.field_size_limit(sys.maxsize)
    if doAppend:
        mode = 'ab'
    else:
        mode = 'wb'
       
    try:
        f = open(datafile, mode) 
        writer = unicodecsv.writer(f, delimiter=delimiter, 
                                   encoding=encoding)
    
    except Exception, e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return writer, f


# .............................................................................
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

# ...............................................
def getLine(csvreader, recno):
    '''
    @summary: Return a line while keeping track of the line number and errors
    '''
    success = False
    line = None
    while not success and csvreader is not None:
        try:
            line = csvreader.next()
            if line:
                recno += 1
            success = True
        except OverflowError, e:
            recno += 1
            print( 'Overflow on record {}, line {} ({})'
                                 .format(recno, csvreader.line, str(e)))
        except StopIteration:
            print('EOF after record {}, line {}'
                                .format(recno, csvreader.line_num))
            success = True
        except Exception, e:
            recno += 1
            print('Bad record on record {}, line {} ({})'
                                .format(recno, csvreader.line, e))

    return line, recno

# # ...............................................
# def openForReadWrite(fname, numKeys=1, numVals=1, header=None, 
#                             delimiter='\t'):
#     '''
#     @summary: Read and populate lookup table if file exists, open and return
#                  file and csvwriter for writing or appending. If lookup file 
#                  is new, write header if provided.
#     '''
#     lookupDict = {}
#     doAppend = False
#     
#     if os.path.exists(fname):
#         doAppend = True
#         colCount = 2
#         if header is not None:
#             colCount = len(header)
#         if (numKeys + numVals) != colCount:
#             raise Exception('Column count != keys + vals')
#         keyIdxs = []
#         valIdxs = []        
#         for i in range(colCount):
#             if i < numKeys:
#                 keyIdxs.append(i)
#             else:
#                 valIdxs.append(i)
#         
#         try:
#             csvRdr, infile = getCSVReader(fname, delimiter)
#             # get header
#             line, recno = self.getLine(csvRdr, 0)
#             # read lookup vals into dictionary
#             while (line is not None):
#                 line, recno = self.getLine(csvRdr, recno)
#                 if line and len(line) == len(valIdxs):
#                     try:
#                         # read one or more values
#                         if len(valIdxs) == 1:
#                             val = line[valIdxs[0]]
#                         else:
#                             val = [line[v] for v in valIdxs]
#                         # read one or more keys
#                         for k in range(numKeys):
#                             lookupDict[line[k]] = val
#                     except Exception, e:
#                         self._log.warn('Failed to read line {} from {}'
#                                             .format(recno, fname))
#             print('Read lookup file {}'.format(fname))
#         finally:
#             infile.close()
#     
#     outWriter, outfile = getCSVWriter(fname, delimiter, doAppend=doAppend)
#     print('Re-opened lookup file {} for appending'.format(fname))
# 
#     if not doAppend and header is not None:
#         outWriter.writerow(header)
#     
#     return lookupDict, outWriter, outfile 

