import os

from common.constants import ENCODING #, BISON_PROVIDER_HEADER
from common.tools import getLogger, getCSVReader, getCSVWriter

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
    def __init__(self, inputFilename, delimiter, sort_col, logname):
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
        self.sort_col = sort_col
        try:
            sort_idx = int(sort_col)
            self.sort_idx = sort_idx
        except:
            self.sort_idx = None
        self.header = []

        basepath, _ = os.path.splitext(inputFilename)
        pth, dataname = os.path.split(basepath)

        logfname = os.path.join(pth, '{}.log'.format(logname))
        self._log = getLogger(logname, logfname)
                
        self.pth = pth
        self.splitBase = os.path.join(pth, 'split_{}'.format(dataname))
        self.tidyfile = os.path.join(pth, 'tidy_{}.csv'.format(dataname))
        self._files = {}
        
    
    # ...............................................
    def close(self):
        for fname, f in self._files:
            f.close()
            self._files.pop(fname)
        
    # ...............................................
    def closeOne(self, fname):
        self._files[fname].close()
        self._files.pop(fname)
        
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
                self._log.warn('First column {} is not an integer on record {}'
                                  .format(row[self.sort_idx], reader.line_num))
            else:
                if gbifid >= currid:
                    writer.writerow(row)
                else:
                    self._log.info('Start new chunk on record {}'
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
    def _get_sortidxs(self, reader, sort_cols):
        """
        @summary: Sort file
        """
        if not self.header:
            self.header = next(reader)
        sort_idxs = []
        for col in sort_cols:
            try:
                idx = self.header.index(col)
            except Exception as e:
                self._log.error('Failed to find column {} in header {}'
                                .format(self.sort_col, self.header))
                raise
            else:
                sort_idxs.append(idx)
                self._log.info('Sort col/index {}/{} in first line contains {}'
                           .format(col, idx, self.header[idx]))
        return sort_idxs

    # ...............................................
    def _read_sortvals(self, sort_cols):
        """
        @summary: Sort file
        """
        self._log.info('Gathering unique sort values from file {}'.format(self.messyfile))
        reader, inf = getCSVReader(self.messyfile, self.delimiter, ENCODING)        

        sort_idxs = self._get_sortidxs(reader, sort_cols)
        sortvals = set()
        try:  
            for row in reader:
                vals = []
                for idx in sort_idxs:
                    vals.append(row[idx])
                sortvals.add(tuple(vals))
        except Exception as e:
            self._log.error('Exception reading infile {}: {}'
                           .format(self.messyfile, e))
        finally:
            inf.close()
        self._log.info('File contained {} unique sort values'
                      .format(len(sortvals)))
        return sortvals

#     # ...............................................
#     def split_by_sortvals(self):
#         """
#         @summary: Sort file
#         """
#         self._log.info('Splitting file {} into files with unique sort values'.format(self.messyfile))
#         reader, inf = getCSVReader(self.messyfile, self.delimiter, ENCODING)
#         sort_idxs = self._get_sortidxs(reader, sort_cols)
#         sortvals = self._read_sortvals()
# 
#         self._log.info('Writing unique sort values to file {}'.format(outfname))
#         if os.path.exists(outfname):
#             fmode = 'a'
#         else:
#             fmode = 'w'
#         writer, outf = getCSVWriter(outfname, self.delimiter, ENCODING, fmode)
#         try:  
#             for row in reader:
#                 vals = []
#                 reccount += 1
#                 for idx in sort_idxs:
#                     vals.append(row[idx])
#                 sortvals.add(tuple(vals))
#         except Exception as e:
#             self._log.error('Exception reading infile {}: {}'
#                            .format(self.messyfile, e))
#         finally:
#             inf.close()
#         
#         try:
#             for val in sortvals:
#                 writer.writerow([val])
#         except Exception as e:
#             self._log.error('Exception writing outfile {}: {}'.format(outfname, e))
#         finally:
#             outf.close()
#         self._log.info('Wrote {} values to {}'.format(len(sortvals, outfname)))
#                         
#     # ...............................................
#     def gather_bison_sortvals(self, outfname, sort_cols, header=[]):
#         """
#         @summary: Sort file
#         """
#         self._log.info('Gathering unique sort values from file {}'.format(self.messyfile))
#         self.header = header
#         sortvals = self._read_sortvals(sort_cols)
# 
#         self._log.info('Writing unique sort values to file {}'.format(outfname))
#         if os.path.exists(outfname):
#             fmode = 'a'
#         else:
#             fmode = 'w'
#         writer, outf = getCSVWriter(outfname, self.delimiter, ENCODING, fmode)
#         try:
#             for vals in sortvals:
#                 writer.writerow(vals)
#         except Exception as e:
#             self._log.error('Exception writing outfile {}: {}'.format(outfname, e))
#         finally:
#             outf.close()
#         self._log.info('Wrote {} values to {}'.format(len(sortvals), outfname))
                        
    # ...............................................
    def _get_provider_file(self, resource_id, resource_url, unique_providers):
        """
        @summary: Sort file
        """
        try:
            writer, outf = unique_providers[(resource_id, resource_url)]
        except:
            outfname = os.path.join(self.pth, resource_id.replace(',', '_')+'.csv')
            if os.path.exists(outfname):
                fmode = 'a'
            else:
                fmode = 'w'
            writer, outf = getCSVWriter(outfname, self.delimiter, ENCODING, fmode)
            self._files[outfname] = outf
        return writer

#     # ...............................................
#     def split_bison_by_sortvals(self, outfname):
#         """
#         @summary: Sort file
#         """
#         sort_cols=['resource_id', 'resource_url'] 
#         self.header=BISON_PROVIDER_HEADER
#         
#         self._log.info('Gathering unique sort values from file {}'.format(self.messyfile))
#         reader, inf = getCSVReader(self.messyfile, self.delimiter, ENCODING)        
# 
#         rid_idx, rurl_idx = self._get_sortidxs(reader, ['resource_id', 'resource_url'])
# 
#         unique_providers = {}
#         try:  
#             for row in reader:
#                 rid = row[rid_idx]
# #                 rurl = row[rurl_idx]
#                 writer = self._get_provider_file(rid, unique_providers)
#                 writer.writerow(row)
#         except Exception as e:
#             self._log.error('Exception reading infile {}: {}'
#                            .format(self.messyfile, e))
#         finally:
#             inf.close()
# 
#         writer, outf = getCSVWriter(outfname, self.delimiter, ENCODING, fmode)
#         try:
#             for vals in sortvals:
#                 writer.writerow(vals)
#         except Exception as e:
#             self._log.error('Exception writing outfile {}: {}'.format(outfname, e))
#         finally:
#             outf.close()
#         self._log.info('Wrote {} values to {}'.format(len(sortvals), outfname))
#     # ...............................................
#     def group(self, outfname):
#         """
#         @summary: Group records  file
#         """
#         self._log.info('Grouping data by column {} into file {}'
#                       .format(self.sort_idx, self.tidyfile))
#         reccount = 0
#         reader, inf = getCSVReader(self.messyfile, self.delimiter, ENCODING)
#         if os.path.exists(outfname):
#             fmode = 'a'
#         else:
#             fmode = 'w'
#         writer, outf = getCSVWriter(outfname, self.delimiter, ENCODING, 'w')
#         self._openfiles[self.mfile] = outf
#         if has_header:
#             header = next(reader)
#             self._log.info('Sort index {} in first line contains {}'
#                                .format(self.sort_idx, header[self.sort_idx]))
#         sortvals = set()          
#         currid = 0
#         for row in reader:
#             reccount += 1
#             sv = row[self.sort_idx]
#             sortvals.add(sv)
#                     
#         self._log.info('File contained {} records'.format(reccount))
#         self.closeOne(self.tidyfile)
                        
    # ...............................................
    def test(self):
        """
        @summary: Test merged/sorted file
        """
        self._log.info('Testing file {}'.format(self.tidyfile))
        reccount = 0
        reader, outf = getCSVReader(self.tidyfile, self.delimiter, ENCODING)
        self._openfiles[self.tidyfile] = outf
        header = next(reader)
        if header[self.sort_idx] != 'gbifID':
            self._log.error('Bad header in {}'.format(self.tidyfile))
            
        currid = 0
        for row in reader:
            reccount += 1
            try:
                gbifid = int(row[self.sort_idx])
            except:
                self._log.error('Bad gbifID on rec {}'.format(reader.line_num))
            else:
                if gbifid < currid:
                    self._log.error('Bad sort gbifID {} on rec {}'.format(gbifid, reader.line_num))
                    break
                elif gbifid == currid:
                    self._log.error('Duplicate gbifID {} on rec {}'.format(gbifid, reader.line_num))
                else:
                    currid = gbifid
                    
        self._log.info('File contained {} records'.format(reccount))
        self.closeOne(self.tidyfile)
                        

# .............................................................................
if __name__ == "__main__":
    # inputFilename, delimiter, sort_index, logname)
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Sort a CSV dataset on a given field"""))
    parser.add_argument('unsorted_file', type=str, 
                        help='Absolute pathname of the input delimited text file' )
    parser.add_argument('command', type=str, 
                        help="""Processing stage:
                        - Split (only used for GBIF downloads with a file 
                          containing multiple sorted sections)
                        - Gather: gather unique sort values to identify how best
                          to proceed.
                        - Merge: merge multiple sorted files into a single 
                          sorted file.
                        """ )
    parser.add_argument('--delimiter', type=str, default=',',
                        help='Delimiter between fields for input file')
    parser.add_argument('--sort_column', type=str, default='gbifId',
                        help='Index or column name of field for data sorting')
    args = parser.parse_args()
    unsorted_file = args.unsorted_file
    cmd = args.command
    delimiter = args.delimiter
    sort_col = args.sort_column

    if not os.path.exists(unsorted_file):
        print ('Input CSV file {} does not exist'.format(unsorted_file))
    else:
        scriptname, ext = os.path.splitext(os.path.basename(__file__))
        
        pth, fname = os.path.split(unsorted_file)
        dataname, ext = os.path.splitext(fname)
        logname = '{}_{}_{}.log'.format(scriptname, dataname, cmd)        

        gf = Sorter(unsorted_file, delimiter, sort_col, logname)
         
        try:
            if cmd  == 'split':
                gf.split()
            elif cmd  == 'gather_bison':
                outfname = os.path.join(pth, 'unique_bison_providers.txt')
#                 gf.gather_bison_sortvals(outfname, 
#                                          sort_cols=['provider', 'resource_id', 'resource_url'], 
#                                          header=BISON_PROVIDER_HEADER)
            elif cmd  == 'split_bison':
                outfname = os.path.join(pth, 'unique_bison_providers.txt')
                gf.split_bison_by_sortvals()
            elif cmd  == 'group':
                gf.sort()
            elif cmd  == 'merge':
                gf.merge()
            elif cmd  == 'test':
                gf.test()
        finally:
            gf.close()

"""
infile = '/tank/data/bison/2019/ancillary/bison_lines_1-10000001.csv'

python3.6 /state/partition1/git/bison/src/common/csvsort.py \
          /tank/data/bison/2019/ancillary/bison_lines_1-10000001.csv \
          sort
          --delimiter=$
          --sort_index=10
"""
