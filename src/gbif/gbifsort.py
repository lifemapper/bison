import os
import sys

from tools import getCSVReader, getCSVWriter, getLogger
from constants import DELIMITER

SORT_IDX = 0

# ...............................................
def usage():
    output = """
    Usage:
        gbifsort infile [split | sort | merge | check]
    """
    print output
    exit(-1)

    # ..........................................................................
class GbifFixer(object):
    # ...............................................
    def __init__(self, inputFilename, delimiter):
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
        self.log = None

        basepath, _ = os.path.splitext(inputFilename)
        pth, dataname = os.path.split(basepath)
#         scriptname, _ = os.path.splitext(os.path.basename(__file__))
#         logfname = os.path.join(pth, '{}_{}.log'.format(scriptname, dataname))        
#         log = getLogger(scriptname, logfname)
        
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
    def _openReader(self, fname):
        if not os.path.exists(fname):
            raise Exception('Input file {} does not exist'.format(fname))
        reader, outf = getCSVReader(fname, self.delimiter)
        self._openfiles[fname] = outf
        return reader

    # ...............................................
    def _openWriter(self, fname):
        writer, outf = getCSVWriter(fname, self.delimiter)
        self._openfiles[fname] = outf
        return writer

    # ...............................................
    def _switchOutput(self, currname, basename, idx):
        # close this chunk and start new
        self.closeOne(currname)
        idx += 1
        newname = '{}_{}.csv'.format(basename, idx)
        writer = self._openWriter(newname)
        return writer, newname, idx

    # ...............................................
    def _setLogger(self, logfname):
        scriptname, ext = os.path.splitext(os.path.basename(__file__))
        self.log = getLogger(scriptname, logfname)
        

    # ...............................................
    def split(self):
        """
        @summary: Split original data file into multiple sorted files.
        @note: Replicate the original header on each smaller sorted file
        """
        self._setLogger(self.splitBase + '.log')

        reader = self._openReader(self.messyfile)
        header = next(reader)

        splitIdx = 0
        splitname = '{}_{}.csv'.format(self.splitBase, splitIdx)
        writer = self._openWriter(splitname)
        writer.writerow(header)

        currid = -1
        for row in reader:
            currid += 1
            try:
                gbifid = int(row[SORT_IDX])
            except Exception, e:
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
            
    # ...............................................
    def _getSplitReadersFirstRecs(self):
        """
        @summary: Find, open, and get CSVReaders for all split files.
        """
        rdrRecs = {}
        idx = 0
        splitname = '{}_{}.csv'.format(self.splitBase, idx)
        while os.path.exists(splitname):
            reader = self._openReader(splitname)
            row = next(reader)
            # If header is present, first field will not be an integer, 
            # so move to the next record
            try:
                int(row[SORT_IDX])
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
                currId = int(rec[SORT_IDX])
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
        self._setLogger(self.tidyfile.replace('csv', 'log'))
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
        self._setLogger(self.tidyfile.replace('.csv', '_test.log'))

        self.log.info('Testing file {}'.format(self.tidyfile))
        reccount = 0
        reader = self._openReader(self.tidyfile)
        header = next(reader)
        if header[SORT_IDX] != 'gbifID':
            self.log.error('Bad header in {}'.format(self.tidyfile))
            
        currid = 0
        for row in reader:
            reccount += 1
            try:
                gbifid = int(row[SORT_IDX])
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
    if len(sys.argv) in (3, 4):
        datafname = sys.argv[1]
        cmd = sys.argv[2]
        if len(sys.argv) == 4:
            delimiter = sys.argv[3]
            if delimiter != ',':
                delimiter = DELIMITER
        else:
            delimiter = DELIMITER
    else:
        usage()
    if cmd not in ('split', 'merge', 'test'):    
        usage()
         
    if not os.path.exists(datafname):
        print ('Input CSV file {} does not exist'.format(datafname))
              
    gf = GbifFixer(datafname, delimiter)
     
    try:
        if cmd  == 'split':
            gf.split()
        elif cmd  == 'merge':
            gf.merge()
        elif cmd  == 'test':
            gf.test()
    finally:
        gf.close()

#         if cmd in ('sort', 'all'):                  
#             # Sort smaller files
#             sortFiles(groupByIdx, pth, unsortedPrefix, sortedPrefix, basename)
#         occparser.close()
# 
#     if cmd in ('merge', 'all'):    
#         # Merge all data for production system into multiple subset files
#         mergeSortedFiles(log, mergefname, pth, sortedPrefix, basename, 
#                                metafname, maxFileSize=None)
# 
#     if cmd == 'check':
#         # Check final output (only for single file now)
#         checkMergedFile(log, mergefname, metafname)


"""
python src/gbif/gbifsort.py  /tank/data/input/bison/territories/verbatim.txt test


import os
import sys

from src.gbif.tools import getCSVReader, getCSVWriter, getLogger
from src.gbif.constants import DELIMITER

from src.gbif.gbifsort import GbifFixer
datafname = '/tank/data/input/bison/territories/verbatim.txt'
cmd = 'split'

basepath, ext = os.path.splitext(datafname)
pth, dataname = os.path.split(basepath)
scriptname = 'gbifsort'
logfname = os.path.join(pth, '{}_{}_{}.log'.format(scriptname, dataname, cmd))        
log = getLogger(scriptname, logfname)

gf = GbifFixer(datafname, DELIMITER, log)

rdrRecs = gf._getSplitReadersFirstRecs()

currId = 0
smId = 0
smRdr = None
smRec = None
for fname, (rdr, rec) in rdrRecs.iteritems():
    currId = int(rec[SORT_IDX])
    if currId < smId:
        smId = currId
        smRdr = rdr
        smFname = fname
smRec = rdrRecs[smRdr]
# Increment reader or close if EOF
try:
    rdrRecs[smRdr] = smRdr.next()
except StopIteration:
    self._openfiles[smFname].close()
    rdrRecs.pop(smFname)

rec = gf._getHeader()
while rec is not None:
    writer.writerow(rec)
    rec = gf._getSmallestRec(rdrRecs)
gf.closeOne(gf.tidyfile)

inf.close()
outf.close()

# gf.split()


COUNTER=100
while [  $COUNTER -lt 1000 ]; do
    let COUNTER=COUNTER+1 
    echo split_multimedia_$COUNTER*
    for i in $( ls /tank/data/input/bison/us/split_multimedia_$COUNTER* ); do
        rm -f $i
    done
done

"""