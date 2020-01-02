import os
import sys

from common.constants import ENCODING
from common.tools import getLogger, getCSVReader, getCSVWriter
from gbif.constants import GBIF_DELIMITER

# ...............................................
def usage():
    output = """
    Usage:
        gbifsort infile [split | sort | merge | check]
    """
    print(output)
    exit(-1)

    # ..........................................................................
class Sorter(object):
    # ...............................................
    def __init__(self, inputFilename, delimiter, sort_index, logname):
        """
        @param inputFilename: full pathname of a CSV file containing records 
                 uniquely identified by the first field in a record.
        @param delimiter: separator between fields of a record
        @param log: a logger to write debugging messages to a file
        @note: Original data file is composed of ~100 chunks of records, each
             chunk sorted on the gbif ID, in position 0 of each record.
    
        """
        self.messyfile = inputFilename
        self.delimiter = delimiter
        self.sort_idx = sort_index

        basepath, _ = os.path.splitext(inputFilename)
        pth, dataname = os.path.split(basepath)

        logfname = os.path.join(pth, '{}.log'.format(logname))
        self._log = getLogger(logname, logfname)
                
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
        @summary: Split original data file into multiple sorted files.
        @note: Replicate the original header on each smaller sorted file
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
                gbifid = int(row[self.sort_idx])
            except Exception:
                self.log.warn('First column {} is not an integer on record {}'
                                  .format(row[self.sort_idx], reader.line_num))
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
            
    # ...............................................
    def _getSplitReadersFirstRecs(self):
        """
        @summary: Find, open, and get CSVReaders for all split files.
        """
        rdrRecs = {}
        idx = 0
        splitname = '{}_{}.csv'.format(self.splitBase, idx)
        while os.path.exists(splitname):
            reader, outf = getCSVReader(splitname, self.delimiter, ENCODING)
            self._openfiles[splitname] = outf
            row = next(reader)
            # If header is present, first field will not be an integer, 
            # so move to the next record
            try:
                int(row[self.sort_idx])
            except:
                row = next(reader)

            rdrRecs[splitname] = (reader, row)
            # increment file
            idx += 1
            splitname = '{}_{}.csv'.format(self.splitBase, idx)
        return rdrRecs

    # ...............................................
    def _getHeader(self):
        reader, inf = getCSVReader(self.messyfile, self.delimiter)
        header = next(reader)
        inf.close()
        return header
    
    # ...............................................
    def _getSmallestRec(self, rdrRecs):
        smRdr = smRec = smId = None
        if len(rdrRecs) > 0:
            for fname, (rdr, rec) in rdrRecs.iteritems():
                currId = int(rec[self.sort_idx])
                if smId is None or currId < smId:
                    smId = currId
                    smRdr = rdr
                    smFname = fname
                    smRec = rec
            # Increment reader or close if EOF
            try:
                newrec = smRdr.next()
            except StopIteration:
                self._openfiles[smFname].close()
                rdrRecs.pop(smFname)
            else:
                rdrRecs[smFname] = (smRdr, newrec)
        return smRec

    # ...............................................
    def merge(self):
        """
        @summary: Merge sorted files into a single larger sorted file.
        """
        rdrRecs = self._getSplitReadersFirstRecs()
        writer, outf = getCSVWriter(self.tidyfile, self.delimiter)
        self._openfiles[self.tidyfile] = outf

        rec = self._getHeader()
        while rec is not None:
            writer.writerow(rec)
            rec = self._getSmallestRec(rdrRecs)
        self.closeOne(self.tidyfile)
                        
    # ...............................................
    def test(self):
        """
        @summary: Test merged/sorted file
        """
        self.log.info('Testing file {}'.format(self.tidyfile))
        reccount = 0
        reader, outf = getCSVReader(self.tidyfile, self.delimiter, ENCODING)
        self._openfiles[self.tidyfile] = outf
        header = next(reader)
        if header[self.sort_idx] != 'gbifID':
            self.log.error('Bad header in {}'.format(self.tidyfile))
            
        currid = 0
        for row in reader:
            reccount += 1
            try:
                gbifid = int(row[self.sort_idx])
            except:
                self.log.error('Bad gbifID on rec {}'.format(reader.line_num))
            else:
                if gbifid < currid:
                    self.log.error('Bad sort gbifID {} on rec {}'.format(gbifid, reader.line_num))
                    break
                elif gbifid == currid:
                    self.log.error('Duplicate gbifID {} on rec {}'.format(gbifid, reader.line_num))
                else:
                    currid = gbifid
                    
        self.log.info('File contained {} records'.format(reccount))
        self.closeOne(self.tidyfile)
                        

# .............................................................................
if __name__ == "__main__":
    delimiter = GBIF_DELIMITER 
    if len(sys.argv) in (3, 4):
        datafname = sys.argv[1]
        cmd = sys.argv[2]
        if len(sys.argv) == 4:
            delimiter = sys.argv[3]
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

        gf = Sorter(datafname, delimiter, log)
         
        try:
            if cmd  == 'split':
                gf.split()
            elif cmd  == 'merge':
                gf.merge()
            elif cmd  == 'test':
                gf.test()
        finally:
            gf.close()

