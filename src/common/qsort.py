import os
import sys
import unicodecsv

from gbif.tools import getLogger, getCSVReader, getCSVWriter
from gbif.constants import IN_DELIMITER, ENCODING

SORT_IDX = 0

# ...............................................
def usage():
    output = """
    Usage:
        qsort infile 
    """
    print(output)
    exit(-1)

# ..........................................................................
class QSorter(object):
    """
    @note: To chunk the file into more easily managed small files (i.e. fewer 
             GBIF API queries), split using sed command output to file like: 
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-500 are lines to delete, and 1500 is the line on which to stop.
             
             sed -n '1,500000p; 500001q' multimedia.txt > multimedia_1-500000.csv
             sed -n '10000000,10000020p; 10000021q' filename
             https://serverfault.com/questions/133692/how-to-display-certain-lines-from-a-text-file-in-linux
    """
    # ...............................................
    def __init__(self, inputFilename, delimiter, log):
        """
        @param inputFilename: full pathname of a CSV file containing records 
                 uniquely identified by the first field in a record.
        @param delimiter: separator between fields of a record
        @param log: a logger to write debugging messages to a file
    
        """
        self.messyfile = inputFilename
        self.delimiter = delimiter
        self.log = None
        unicodecsv.field_size_limit(sys.maxsize)

        basepath, _ = os.path.splitext(inputFilename)
        pth, dataname = os.path.split(basepath)
        self.log = log
                
        self.pth = pth
        self.splitBase = os.path.join(pth, 'split_{}'.format(dataname))
        self.tidyfile = os.path.join(pth, 'tidy_{}.csv'.format(dataname))
        self._openfiles = {}
        
    
    # ...............................................
    def close(self):
        for f in self._openfiles.values():
            f.close()
        self._openfiles = {}
        
    # ...............................................
    def closeOne(self, fname):
        self._openfiles[fname].close()
        self._openfiles.pop(fname)
        
    # ...............................................

    # ...............................................
    def _switchOutput(self, currname, basename, idx):
        # close this chunk and start new
        self.closeOne(currname)
        idx += 1
        newname = '{}_{}.csv'.format(basename, idx)
        # Get writer and save open file for later closing
        writer, outf = getCSVWriter(newname, self.delimiter, ENCODING, 
                                    doAppend=True)
        self._openfiles[newname] = outf

        return writer, newname, idx

    # ...............................................
    def split(self):
        """
        @summary: Split original data file into multiple smaller files.
        @note: Replicate the original header on each smaller file
        """
        reader, outf = getCSVReader(self.messyfile, self.delimiter, ENCODING)
        self._openfiles[self.messyfile] = outf
        header = next(reader)

        splitIdx = 0
        splitname = '{}_{}.csv'.format(self.splitBase, splitIdx)
        writer, outf = getCSVWriter(splitname, self.delimiter, ENCODING, 
                                    doAppend=True)
        self._openfiles[splitname] = outf        
        writer.writerow(header)

        currid = -1
        for row in reader:
            currid += 1
            try:
                gbifid = int(row[SORT_IDX])
            except Exception:
                self.log.warn('First column {} is not an integer on record {}'
                                  .format(row[SORT_IDX], reader.line_num))
            else:
                if gbifid >= currid:
                    writer.writerow(row)
                else:
                    self.log.info('Start new chunk on record {}'
                                      .format(reader.line_num))
                    # close this chunk and start new
                    writer, splitname, splitIdx = \
                            self._switchOutput(splitname, self.splitBase, splitIdx)
                    writer.writerow(header)
                    writer.writerow(row)
                currid = gbifid
        self.closeOne(self.messyfile)

# .............................................................................
if __name__ == "__main__":    
    if len(sys.argv) in (3, 4):
        datafname = sys.argv[1]
        cmd = sys.argv[2]
        if len(sys.argv) == 4:
            delimiter = sys.argv[3]
            if delimiter != ',':
                delimiter = IN_DELIMITER
        else:
            delimiter = IN_DELIMITER
    else:
        usage()
    if cmd not in ('split', 'merge', 'test'):    
        usage()
         
    if not os.path.exists(datafname):
        print ('Input CSV file {} does not exist'.format(datafname))
    else:
        scriptname, ext = os.path.splitext(os.path.basename(__file__))
        
        pth, fname = os.path.split(datafname)
        dataname, ext = os.path.splitext(fname)
        logfname = os.path.join(pth, '{}_{}_{}.log'.format(scriptname, dataname, cmd))        
        log = getLogger(scriptname, logfname)

        gf = QSorter(datafname, delimiter, log)
         
        try:
            if cmd  == 'split':
                gf.split()
            elif cmd  == 'merge':
                gf.merge()
            elif cmd  == 'test':
                gf.test()
        finally:
            gf.close()
            
            
"""
fname = 'multimedia_1-500000.csv'
"""            
