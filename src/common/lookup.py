import os
from pympler import asizeof

from common.constants import ENCODING
from common.tools import (getCSVReader, getCSVDictReader, getCSVWriter, getLine)

class VAL_TYPE:
    DICT = 1
    LIST = 2
#     FLOAT = 3
#     INT = 4
#     STRING = 5

# .............................................................................
class Lookup(dict):
    """
    @summary: 
    @note: 
    """
    # ...............................................
    def __init__(self, valtype=VAL_TYPE.DICT, encoding=ENCODING):
        """
        @summary: Constructor
        """
        self.data = {}
        self.valtype = valtype
        self.encoding = encoding
        
    # ...............................................
    def save_to_lookup(self, key, val, is_list=False):
        """
        @summary: Save scientificName / taxonKeys for parse or query 
        @param rec: dictionary of all fieldnames and values for this record
        @note: The GBIF name parser fails on unicode namestrings
        @note: Invalid records with no scientificName or taxonKey were 
               discarded earlier in self._clean_input_values
        @note: Names saved to dictionary key=sciname, val=[taxonid, ...] 
               to avoid writing duplicates.  
        """
        if key is None and val is None:
            self._log.warning('Missing key {} or value {}'.format(key, val))
        else:
            try:
                existingval = self[key]
            except KeyError:
                if is_list:
                    self.data[key] = [val]
                else:
                    self.data[key] = val
            else:
                if is_list:
                    self.data[key] = existingval.append(val)
                else:
                    raise Exception('Conflicting value for key {} and existing val {}')
                

    # ...............................................
    def write_lookup(self, fname, header, delimiter, encoding=ENCODING):
        # Write scientific names and taxonKeys found with them in raw data
        fmode = 'w'
        if os.path.exists(fname):
            fmode = 'a'
        try:
            writer, outf = getCSVWriter(fname, delimiter, encoding, fmode=fmode)
            self._log.info('Opened file {} for write'.format(fname))        
            if fmode == 'w' and header is not None:
                writer.writerow(header)
                for key, val in self.data.items():
                    row = [k for k in val]
                    row.insert(0, key)
                    writer.writerow(row)
        except Exception:
            self._log.error('Failed to write data to {}'.format(fname))
        finally:
            outf.close()

    # ...............................................
    def read_lookup(self, fname, keyfld, delimiter, encoding=ENCODING):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  val = dictionary of record values
        '''
        if os.path.exists(fname):
            if self.valtype == VAL_TYPE.DICT:
                try:
                    rdr, inf = getCSVDictReader(fname, delimiter, encoding)
                except Exception as e:
                    self._log.error('Failed reading data in {}: {}'
                                    .format(fname, e))
                else:
                    for data in rdr:
                        datakey = data.pop(keyfld)
                        self.data[datakey] = data
                finally:
                    inf.close()
                    
            elif self.valtype == VAL_TYPE.LIST:
                recno = 0
                try:
                    rdr, inf = getCSVReader(fname, delimiter, encoding)
                    # get header
                    line, recno = getLine(rdr, recno)
                    # read lookup vals into dictionary
                    while (line is not None):
                        line, recno = getLine(rdr, recno)
                        if line and len(line) > 0:
                            try:
                                # First item is scientificName, rest are taxonKeys
                                self.data[line[0]] = list(line[1:])
                            except Exception:
                                self._log.warn('Failed to parse line {} {}'
                                               .format(recno, line))
                except Exception as e:
                    self._log.error('Failed reading data in {}: {}'
                                    .format(fname, e))
                finally:
                    inf.close()
        if self.data:
            print('Lookup table size for {}:'.format(fname))
            print(asizeof.asized(self.data).format())

    
