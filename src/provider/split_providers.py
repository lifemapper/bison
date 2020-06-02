import os

from common.constants import ENCODING #, BISON_PROVIDER_HEADER
from common.tools import getLogger, get_csv_reader, get_csv_writer

# ...............................................
def usage():
    output = """
    Usage:
        gbifsort infile [split | sort | merge | check]
    """
    print(output)
    exit(-1)

    # ..........................................................................
class DataSplitter(object):
    # ...............................................
    def __init__(self, inputFilename, indelimiter, group_col, logname):
        """
        @param inputFilename: full pathname of a CSV file containing records 
                 uniquely identified by the first field in a record.
        @param delimiter: separator between fields of a record
        @param log: a logger to write debugging messages to a file
        @note: Original data file is composed of ~100 chunks of records, each
             chunk sorted on the gbif ID, in position 0 of each record.
    
        """
        self.messyfile = inputFilename
        self.indelimiter = indelimiter
        self.group_col = group_col
        self.group_idx = None
        try:
            group_idx = int(group_col)
            self.group_idx = group_idx
        except:
            pass
        self.header = []

        tmp, _ = os.path.splitext(self.messyfile)
        self._basepath, self._dataname = os.path.split(tmp)

        logfname = os.path.join(pth, '{}.log'.format(logname))
        self._log = getLogger(logname, logfname)
                
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
    def _open_group_file(self, grpval, out_delimiter):
        basefname = '{}_{}.csv'.format(self._dataname, grpval)
        grp_fname = os.path.join(self._basepath, basefname)
        writer, outf = get_csv_writer(grp_fname, out_delimiter, ENCODING)
        self._files[grp_fname] = outf
        return writer

    # ...............................................
    def gather_groupvals(self, fname):
        """
        @summary: Split original data file with chunks of sorted data into 
                  multiple sorted files.
        @note: Replicate the original header on each smaller sorted file
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
            header = next(reader)
            groups = {}
    
            grpval = None
            grpcount = 0
            for row in reader:
                try:
                    currval = row[self.group_idx]
                except Exception as e:
                    self._log.warn('Failed to get column {} from record {}'
                                      .format(self.group_idx, reader.line_num))
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
            writer, outf = get_csv_writer(fname, self.indelimiter, ENCODING)
            writer.writerow(['groupvalue', 'count'])
            for grpval, grpcount in groups.items():
                writer.writerow([grpval, grpcount])
        except Exception as e:
            pass
        finally:
            outf.close()
            
    # ...............................................
    def write_group_files(self, out_delimiter):
        """
        @summary: Split large file into multiple files, each containing a header
                  and records of a single group value. 
        @note: The number of group files must be small enough for the system to 
               have them all open at the same time.
        @note: Use "gather" to evaluate the dataset first.
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
            header = next(reader)
            if self.group_idx is None:
                try:
                    header.index(self.group_col)
                except:
                    raise Exception('Field {} does not exist in header {}'.format(
                        self.group_col, header))
            # {groupval: csvwriter}
            groupfiles = {}
            for row in reader:
                try:
                    grpval = row[self.group_idx]
                except Exception as e:
                    self._log.warn('Failed to get column {} from record {}'
                                      .format(self.group_idx, reader.line_num))
                else:
                    try:
                        wtr = groupfiles[grpval]
                    except:
                        wtr = self._open_group_file(grpval, out_delimiter)
                        groupfiles[grpval] = wtr
                        wtr.writerow(header)
                    
                    wtr.writerow(row)
        except Exception as e:
            raise
        finally:
            inf.close()

    # ...............................................
    def _getHeader(self):
        reader, inf = get_csv_reader(self.messyfile, self.indelimiter)
        header = next(reader)
        inf.close()
        return header

    # ...............................................
    def _read_sortvals(self, group_cols):
        """
        @summary: Sort file
        """
        self._log.info('Gathering unique sort values from file {}'.format(self.messyfile))
        reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)        

        group_idxs = self._get_sortidxs(reader, group_cols)
        sortvals = set()
        try:  
            for row in reader:
                vals = []
                for idx in group_idxs:
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

                        
    # ...............................................
    def test(self, outdelimiter):
        """
        @summary: Test merged/sorted file
        """
        self._log.info('Testing file {}'.format(self.tidyfile))
        reccount = 0
        reader, outf = get_csv_reader(self.tidyfile, outdelimiter, ENCODING)
        self._files[self.tidyfile] = outf
        header = next(reader)
        if header[self.group_idx] != 'gbifID':
            self._log.error('Bad header in {}'.format(self.tidyfile))
            
        currid = 0
        for row in reader:
            reccount += 1
            try:
                gbifid = int(row[self.group_idx])
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
    # inputFilename, delimiter, group_index, logname)
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Group CSV dataset, optionally into separate files,
                on a given field"""))
    parser.add_argument('infile', type=str, 
                        help='Absolute pathname of the input delimited text file' )
    parser.add_argument('--input_delimiter', type=str, default='$',
                        help='Delimiter between fields for input file')
    parser.add_argument('--output_delimiter', type=str, default='$',
                        help='Delimiter between fields for output file(s)')
    parser.add_argument('--group_column', type=str, default='resource_id',
                        help='Index or column name of field for data grouping')
    args = parser.parse_args()
    unsorted_file = args.infile
    in_delimiter = args.input_delimiter
    out_delimiter = args.output_delimiter
    group_col = args.group_column

    if not os.path.exists(unsorted_file):
        print ('Input CSV file {} does not exist'.format(unsorted_file))
    else:
        scriptname, ext = os.path.splitext(os.path.basename(__file__))
        
        pth, fname = os.path.split(unsorted_file)
        dataname, ext = os.path.splitext(fname)
        logname = '{}_{}.log'.format(scriptname, dataname)        

        gf = DataSplitter(unsorted_file, in_delimiter, group_col, logname)
         
        try:
            gf.write_group_files(out_delimiter)
        finally:
            gf.close()

"""
infile = '/tank/data/bison/2019/ancillary/bison_lines_1-10000001.csv'

python3.6 /state/partition1/git/bison/src/provider/split_providers.py \
          /tank/data/bison/2019/provider/bison.csv \
          --input_delimiter=$
          --group_column=resource_id

import os
from common.constants import ENCODING 
from common.tools import getLogger, get_csv_reader, get_csv_writer

group_idx = 13
group_col = 'resource_id'
messyfile = 'bison.csv'
in_delimiter = '$'

pth, fname = os.path.split(messyfile)
dataname, ext = os.path.splitext(fname)
logname = '{}_{}.log'.format('split_provider', dataname)        

gf = DataSplitter(messyfile, in_delimiter, group_col, logname)
reader, outf = get_csv_reader(messyfile, delimiter, ENCODING)
header = next(reader)

splitIdx = 0

row = next(reader)





"""
