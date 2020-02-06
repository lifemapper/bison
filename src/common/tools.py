import csv
import logging
from logging.handlers import RotatingFileHandler
from sys import maxsize

from common.constants import (LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                              LOGFILE_BACKUP_COUNT, EXTRA_VALS_KEY)

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
def getCSVWriter(datafile, delimiter, encoding, fmode='w'):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    if fmode not in ('w', 'a'):
        raise Exception('File mode must be "w" (write) or "a" (append)')
    
    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding) 
        writer = csv.writer(f, delimiter=delimiter)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return writer, f

# .............................................................................
def getCSVDictReader(datafile, delimiter, encoding, fieldnames=None):
    try:
        f = open(datafile, 'r', encoding=encoding) 
        dreader = csv.DictReader(f, fieldnames=fieldnames, restkey=EXTRA_VALS_KEY,
                                 delimiter=delimiter)        
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    return dreader, f

# .............................................................................
def getCSVDictWriter(datafile, delimiter, encoding, fldnames, fmode='w'):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    if fmode not in ('w', 'a'):
        raise Exception('File mode must be "w" (write) or "a" (append)')
    
    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding) 
        writer = csv.DictWriter(f, fieldnames=fldnames, delimiter=delimiter)
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
def open_csv_files(infname, delimiter, encoding, infields=None,
                   outfname=None, outfields=None, outdelimiter=None):
    '''
    @summary: Open CSV data for reading as a dictionary (assumes a header), 
              new output file for writing (rows as a list, not dictionary)
    @param infname: Input CSV filename.  Either a sequence of fielnames must 
                    be provided as infields or the file must begin with a header
                    to set dictionary keys. 
    @param delimiter: CSV delimiter for input and optionally output 
    @param encoding: Encoding for input and optionally output 
    @param infields: Optional ordered list of fieldnames, used when input file
                     does not contain a header.
    @param outfname: Optional output CSV file 
    @param outdelimiter: Optional CSV delimiter for output if it differs from 
                      input delimiter
    @param outfields: Optional ordered list of fieldnames for output header 
    '''
    # Open incomplete BISON CSV file as input
    print('Open input file {}'.format(infname))
    csv_dict_reader, inf = getCSVDictReader(infname, delimiter, encoding, 
                                            fieldnames=infields)
    # Optional new BISON CSV output file
    csv_writer = outf = None
    if outfname:
        if outdelimiter is None:
            outdelimiter = delimiter
        print('Open output file {}'.format(outfname))
        csv_writer, outf = getCSVWriter(outfname, outdelimiter, encoding)
        # if outfields are not provided, no header
        if outfields:
            csv_writer.writerow(outfields)
    return (csv_dict_reader, inf, csv_writer, outf)
        

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

