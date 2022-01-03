import os

from riis.common import ENCODING #, BISON_PROVIDER_HEADER
from riis.common import get_logger, get_csv_reader, get_csv_writer

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

        tmp, _ = os.path.splitext(self.messyfile)
        self._basepath, self._dataname = os.path.split(tmp)

        logfname = os.path.join(pth, '{}.log'.format(logname))
        self._log = get_logger(logname, logfname)

        self.pth = pth
        self.splitBase = os.path.join(pth, 'split_{}'.format(self._dataname))
        self.tidyfile = os.path.join(pth, 'tidy_{}.csv'.format(self._dataname))
        self._files = {}


    # ...............................................
    def close(self):
        for fname, f in self._files.items():
            f.close()
        self._files = {}

    # ...............................................
    def closeOne(self, fname):
        self._files[fname].close()
        self._files.pop(fname)

    # ...............................................
    def _get_group_file(self, grpval):
        basefname = '{}_{}.csv'.format(self._dataname, grpval)
        grp_fname = os.path.join(self._basepath, basefname)
        writer, outf = get_csv_writer(grp_fname, self.delimiter, ENCODING)
        self._files[grp_fname] = outf
        return writer

    # ...............................................
    def _switchOutput(self, currname, basename, idx):
        # close this chunk and start new
        self.closeOne(currname)
        idx += 1
        newname = '{}_{}.csv'.format(basename, idx)
        # Get writer and save open file for later closing
        writer, outf = get_csv_writer(newname, self.delimiter, ENCODING,
                                    doAppend=True)
        self._files[newname] = outf

        return writer, newname, idx

    # ...............................................
    def gather_groupvals(self, fname):
        """
        @summary: Split original data file with chunks of sorted data into
                  multiple sorted files.
        @note: Replicate the original header on each smaller sorted file
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
            header = next(reader)
            groups = {}

            grpval = None
            grpcount = 0
            for row in reader:
                try:
                    currval = row[self.sort_idx]
                except Exception as e:
                    self._log.warn('Failed to get column {} from record {}'
                                      .format(self.sort_idx, reader.line_num))
                else:
                    if grpval is None:
                        grpval = currval
                    if currval != grpval:
                        self._log.info('Start new group {} on record {}'
                                       .format(currval, reader.line_num))
                        try:
                            groups[grpval] += grpcount
                        except:
                            groups[grpval] = grpcount
                        grpcount = 1
                        grpval = currval
                    else:
                        grpcount += 1
        except Exception as e:
            pass
        finally:
            inf.close()

        try:
            writer, outf = get_csv_writer(fname, self.delimiter, ENCODING)
            writer.writerow(['groupvalue', 'count'])
            for grpval, grpcount in groups.items():
                writer.writerow([grpval, grpcount])
        except Exception as e:
            pass
        finally:
            outf.close()

    # ...............................................
    def write_group_files(self):
        """
        @summary: Split large file into multiple files, each containing a header
                  and records of a single group value.
        @note: The number of group files must be small enough for the system to
               have them all open at the same time.
        @note: Use "gather" to evaluate the dataset first.
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
            header = next(reader)
            # {groupval: csvwriter}
            groupfiles = {}
            for row in reader:
                try:
                    grpval = row[self.sort_idx]
                except Exception as e:
                    self._log.warn('Failed to get column {} from record {}'
                                      .format(self.sort_idx, reader.line_num))
                else:
                    try:
                        wtr = groupfiles[grpval]
                    except:
                        wtr = self._get_group_file(grpval)
                        groupfiles[grpval] = wtr
                        wtr.writerow(header)

                    wtr.writerow(row)
        except Exception as e:
            raise
        finally:
            inf.close()


    # ...............................................
    def split_sorted(self):
        """
        @summary: Split original data file with chunks of sorted data into
                  multiple sorted files.
        @note: Replicate the original header on each smaller sorted file
        """
        reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
        self._files[self.messyfile] = inf
        header = next(reader)

        splitIdx = 0
        splitname = '{}_{}.csv'.format(self.splitBase, splitIdx)
        writer, outf = get_csv_writer(splitname, self.delimiter, ENCODING)
        self._files[splitname] = outf
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
            reader, outf = get_csv_reader(splitname, self.delimiter, ENCODING)
            self._files[splitname] = outf
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
        reader, inf = get_csv_reader(self.messyfile, self.delimiter)
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
                self._files[smFname].close()
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
        writer, outf = get_csv_writer(self.tidyfile, self.delimiter)
        self._files[self.tidyfile] = outf

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
        reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)

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
#         reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
#         sort_idxs = self._get_sortidxs(reader, sort_cols)
#         sortvals = self._read_sortvals()
#
#         self._log.info('Writing unique sort values to file {}'.format(outfname))
#         if os.path.exists(outfname):
#             fmode = 'a'
#         else:
#             fmode = 'w'
#         writer, outf = get_csv_writer(outfname, self.delimiter, ENCODING, fmode)
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
#         writer, outf = get_csv_writer(outfname, self.delimiter, ENCODING, fmode)
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
            writer, outf = get_csv_writer(outfname, self.delimiter, ENCODING, fmode)
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
#         reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
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
#         writer, outf = get_csv_writer(outfname, self.delimiter, ENCODING, fmode)
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
#         reader, inf = get_csv_reader(self.messyfile, self.delimiter, ENCODING)
#         if os.path.exists(outfname):
#             fmode = 'a'
#         else:
#             fmode = 'w'
#         writer, outf = get_csv_writer(outfname, self.delimiter, ENCODING, 'w')
#         self._files[self.mfile] = outf
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
        reader, outf = get_csv_reader(self.tidyfile, self.delimiter, ENCODING)
        self._files[self.tidyfile] = outf
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
                description=("""Group or sort a CSV dataset on a given field"""))
    parser.add_argument('unsorted_file', type=str,
                        help='Absolute pathname of the input delimited text file' )
    parser.add_argument('command', type=str,
                        help="""Processing stage:
                        - gather: gather unique sort/group values in a summary
                          file to identify how best to proceed.
                        - split: create multiple files, each sorted on a single
                          column (only used for GBIF downloads with a file
                          containing multiple sorted sections)
                        - group: create multiple files, each containing only
                          records with a single group value. The number of group
                          files must be small enough for the system to have all
                          open at the same time.  Use "gather" to evaluate the
                          dataset first. (only used for BISON provider data
                          containing ~100 groups)
                        - merge: merge multiple sorted files into a single
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
            # Use 'split' to to write records into individual files, with
            # each file sorted on desired grouping value.  This command assumes
            # that the input file has chunks of pre-sorted data.
            if cmd  == 'split':
                gf.split_sorted()
            # Use 'gather' to identify the number and placement of records for
            # desired grouping value
            elif cmd  == 'gather':
                outfname = os.path.join(pth, 'summary_'+dataname+ext)
                gf.gather_groupvals(outfname)
            # Use 'group' to write records into individual files, with name and
            # contents of each file based on the same grouping value
            elif cmd == 'group':
                gf.write_group_files()
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
          --sort_index=13

import os
from common.constants import ENCODING
from common.tools import get_logger, get_csv_reader, get_csv_writer

sort_idx = 13
messyfile = 'bison.csv'
reader, outf = get_csv_reader(messyfile, delimiter, ENCODING)
header = next(reader)

splitIdx = 0

row = next(reader)





"""
