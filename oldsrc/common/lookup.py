import os

from riis.common import ENCODING
from riis.common import (get_csv_dict_reader, get_csv_dict_writer,
                         get_csv_reader, get_csv_writer, getLine, makerow)

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
    def init_from_dict(cls, lookupdict, valtype=VAL_TYPE.DICT, encoding=ENCODING):
        """
        @summary: Constructor
        """
        lookup = Lookup(valtype=valtype, encoding=encoding)
        lookup.lut = lookupdict
        return lookup

    # ...............................................
    @classmethod
    def init_from_file(cls, lookup_fname, prioritized_keyfld_lst, delimiter,
                       valtype=VAL_TYPE.DICT, encoding=ENCODING,
                       ignore_quotes=True):
        """
        @summary: Constructor
        """
        lookup = Lookup(valtype=valtype, encoding=encoding)
        lookup.read_lookup(
            lookup_fname, prioritized_keyfld_lst, delimiter,
            ignore_quotes=ignore_quotes)
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
#             print('Saving key {}'.format(key))
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
                    print('Ignore conflicting value for key {}, new val {} and existing val {}'
                          .format(key, val, existingval))

    # ...............................................
    def _get_field_names(self):
        fldnames = None
        if self.valtype == VAL_TYPE.DICT:
            fldnames = set()
            for key, vals in self.lut.items():
                for fn, val in vals.items():
                    fldnames.add(fn)
        return fldnames

    # ...............................................
    def write_lookup(self, fname, header, delimiter):
        # Write scientific names and taxonKeys found with them in raw data
        fmode = 'w'
        if os.path.exists(fname):
            fmode = 'a'
        try:
            if self.valtype == VAL_TYPE.DICT:
                # Write all vals in dict, assumes each dictionary-value has the same keys
                if header is None:
                    header = self._get_field_names()
                    writer, outf = get_csv_dict_writer(
                        fname, delimiter, self.encoding, header, fmode=fmode)
                    if fmode == 'w':
                        writer.writeheader()
                    for key, ddict in self.lut.items():
                        writer.writerow(ddict)
                # Write values from dict for header fields, insert '' when missing
                else:
                    writer, outf = get_csv_writer(fname, delimiter, self.encoding,
                                                fmode=fmode)
                    writer.writerow(header)
                    for key, rec in self.lut.items():
                        row = makerow(rec, header)
                        writer.writerow(row)

            # Non-dictionary lookup
            else:
                writer, outf = get_csv_writer(fname, delimiter, self.encoding,
                                            fmode=fmode)
                if fmode == 'w' and header is not None:
                    writer.writerow(header)
                if self.valtype in (VAL_TYPE.SET, VAL_TYPE.TUPLE):
                    for key, val in self.lut.items():
                        row = [k for k in val]
                        row.insert(0, key)
                        writer.writerow(row)
        except Exception as e:
            print('Failed to write data to {}, ({})'.format(fname, e))
        finally:
            outf.close()

    # ...............................................
    def read_lookup(self, fname, prioritized_keyfld_lst, delimiter, ignore_quotes=True):
        '''
        @summary: Read and populate dictionary with key = uuid and
                  val = dictionary of record values
        '''
        no_old_legacy = 0
        no_new_legacy = 0
        if os.path.exists(fname):
            if self.valtype == VAL_TYPE.DICT:
                try:
                    rdr, inf = get_csv_dict_reader(
                        fname, delimiter, self.encoding,
                        ignore_quotes=ignore_quotes)
                except Exception as e:
                    print('Failed reading data in {}: {}'
                                    .format(fname, e))
                else:
                    for data in rdr:
                        for keyfld in prioritized_keyfld_lst:
                            datakey = data[keyfld]
                            if datakey:
                                self.lut[datakey] = data
                                break
                        if not datakey:
                            print('No {} for record {}'.format(keyfld, data))
                finally:
                    inf.close()
                print('no_old_legacy {}  no_new_legacy (default -9999) {}'
                      .format(no_old_legacy, no_new_legacy))

            elif self.valtype == VAL_TYPE.SET:
                recno = 0
                try:
                    rdr, inf = get_csv_reader(fname, delimiter, self.encoding)
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
