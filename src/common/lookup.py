import os

from common.constants import ENCODING
from common.tools import (getCSVDictReader, getCSVDictWriter, 
                          getCSVReader, getCSVWriter, getLine)

class VAL_TYPE:
    DICT = 1
    SET = 2
    TUPLE = 3
    INT = 4
    FLOAT = 5
    STRING = 6

# .............................................................................
class Lookup(object):
    """
    @summary: 
    @note: 
    """
    # ...............................................
    def __init__(self, valtype=VAL_TYPE.DICT, encoding=ENCODING):
        """
        @summary: Constructor
        """
        self.lut = {}
        self.valtype = valtype
        self.encoding = encoding
        
    # ...............................................
    @classmethod
    def initFromDict(cls, lookupdict, valtype=VAL_TYPE.DICT, encoding=ENCODING):
        """
        @summary: Constructor
        """
        lookup = Lookup(valtype=valtype, encoding=encoding)
        lookup.lut = lookupdict
        return lookup

    # ...............................................
    @classmethod
    def initFromFile(cls, lookup_fname, keyfld, delimiter, 
                     valtype=VAL_TYPE.DICT, encoding=ENCODING):
        """
        @summary: Constructor
        """
        lookup = Lookup(valtype=valtype, encoding=encoding)
        lookup.read_lookup(lookup_fname, keyfld, delimiter)
        return lookup

    # ...............................................
    def save_to_lookup(self, key, val):
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
            print('Missing key {} or value {}'.format(key, val))
        else:
            try:
                existingval = self.lut[key]
            except KeyError:
                if self.valtype == VAL_TYPE.SET:
                    self.lut[key] = set([val])
                else:
                    self.lut[key] = val
            else:
                if self.valtype == VAL_TYPE.SET:
                    self.lut[key].add(val)
                elif val != existingval:
                    raise Exception('Conflicting value for key {} and existing val {}')
                

    # ...............................................
    def write_lookup(self, fname, header, delimiter):
        # Write scientific names and taxonKeys found with them in raw data
        fmode = 'w'
        if os.path.exists(fname):
            fmode = 'a'
        try:
            if self.valtype == VAL_TYPE.DICT:
                writer, outf = getCSVDictWriter(fname, delimiter, self.encoding, 
                                                header, fmode=fmode)
                print('Opened file {} for write'.format(fname))
                if fmode == 'w':
                    writer.writeheader()
                row = []
                for key, ddict in self.lut.items():
                    writer.writerow(ddict)
            else:
                writer, outf = getCSVWriter(fname, delimiter, self.encoding, 
                                            fmode=fmode)
                print('Opened file {} for write'.format(fname))
                if fmode == 'w' and header is not None:
                    writer.writerow(header)
                if self.valtype in (VAL_TYPE.SET, VAL_TYPE.TUPLE):
                    for key, val in self.lut.items():
                        row = [k for k in val]
                        row.insert(0, key)
                        writer.writerow(row)
        except Exception:
            print('Failed to write data to {}'.format(fname))
        finally:
            outf.close()

    # ...............................................
    def read_lookup(self, fname, keyfld, delimiter):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  val = dictionary of record values
        '''
        if os.path.exists(fname):
            if self.valtype == VAL_TYPE.DICT:
                try:
                    rdr, inf = getCSVDictReader(fname, delimiter, self.encoding)
                except Exception as e:
                    print('Failed reading data in {}: {}'
                                    .format(fname, e))
                else:
                    for data in rdr:
                        datakey = data.pop(keyfld)
                        self.lut[datakey] = data
                finally:
                    inf.close()
                    
            elif self.valtype == VAL_TYPE.SET:
                recno = 0
                try:
                    rdr, inf = getCSVReader(fname, delimiter, self.encoding)
                    # get header
                    line, recno = getLine(rdr, recno)
                    # read lookup vals into dictionary
                    while (line is not None):
                        line, recno = getLine(rdr, recno)
                        if line and len(line) > 0:
                            try:
                                # First item is scientificName, rest are taxonKeys
                                self.lut[line[0]] = set(line[1:])
                            except Exception:
                                print('Failed to parse line {} {}'
                                               .format(recno, line))
                except Exception as e:
                    print('Failed reading data in {}: {}'
                                    .format(fname, e))
                finally:
                    inf.close()
#         print('Lookup table size for {}:'.format(fname))
#         print(asizeof.asized(self.lut).format())

    
