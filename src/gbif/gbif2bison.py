"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

             Lifemapper Project, lifemapper [at] ku [dot] edu, 
             Biodiversity Institute,
             1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
             This program is free software; you can redistribute it and/or modify 
             it under the terms of the GNU General Public License as published by 
             the Free Software Foundation; either version 2 of the License, or (at 
             your option) any later version.
  
             This program is distributed in the hope that it will be useful, but 
             WITHOUT ANY WARRANTY; without even the implied warranty of 
             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
             General Public License for more details.
  
             You should have received a copy of the GNU General Public License 
             along with this program; if not, write to the Free Software 
             Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
             02110-1301, USA.
"""
import glob
import os
import time
import xml.etree.ElementTree as ET

from gbif.constants import (IN_DELIMITER, OUT_DELIMITER, PROHIBITED_VALS, 
                            INTERPRETED, TERM_CONVERT, ENCODING,
                            ORDERED_OUT_FIELDS, COMPUTE_FIELDS, SAVE_FIELDS,
                            CLIP_CHAR, META_FNAME, NAMESPACE)
from gbif.gbifapi import GbifAPI
from gbif.tools import getCSVReader, getCSVWriter, getLine, getLogger

        
# .............................................................................
class GBIFReader(object):
    """
    @summary: GBIF Record containing CSV record of 
                 * original provider data from verbatim.txt
                 * GBIF-interpreted data from occurrence.txt
    @note: To chunk the file into more easily managed small files (i.e. fewer 
             GBIF API queries), split using sed command output to file like: 
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-500 are lines to delete, and 1500 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, interpreted_fname, meta_fname, outfname):
        """
        @summary: Constructor
        @param interpreted_fname: Full filename containing records from the GBIF 
                 interpreted occurrence table
        @param meta_fname: Full filename containing metadata for all data files in the 
                 Darwin Core GBIF Occurrence download:
                     https://www.gbif.org/occurrence/search
        @param outfname: Full filename for the output BISON CSV file
        """
        self._gbif_delimiter = IN_DELIMITER
        self._bison_delimiter = OUT_DELIMITER
        self._encoding = ENCODING
        
        self.gbifapi = None
        
        self._files = []
        
        # Interpreted GBIF occurrence file
        self.interp_fname = interpreted_fname
        inpth, _ = os.path.split(interpreted_fname)
        self._dataset_pth = os.path.join(inpth, 'dataset')
        self._inf = None
        self._reader = None
        # GBIF metadata file for occurrence files
        self._meta_fname = meta_fname
        self.fldMeta = None
        
        # Output BISON occurrence file
        self.outfname = outfname
        self._outf = None
        self._writer = None
        outpth, outbasename = os.path.split(outfname)
        outbase, _ = os.path.splitext(outbasename)
        
        # Input for Canonical name lookup
        self.namekey_fname = os.path.join(outpth, 'namekey_list.csv')
        self._namef = None
        self._namekey_writer = None
        self._namekey = {}

        # Input for Publisher lookup (providerID)
        self.pub_fname = os.path.join(outpth, 'publisher_list.txt')
        self._pubf = None
        self._pubset = None

        # Input for Dataset lookup (datasetKey)
        self.dsk_fname = os.path.join(outpth, 'datasetkey_list.txt')
        self._dskf = None
        self._dskset = set()

        logname, _ = os.path.splitext(os.path.basename(__file__))
        logfname = os.path.join(outpth, outbase + '.log')
        if os.path.exists(logfname):
            ts = int(time.time())
            logfname = os.path.join(outpth, outbase + '.log.{}'.format(ts))
        self._log = getLogger(logname, logfname)
        
#     # ...............................................
#     def _time_stamp(self, msg):
#         ts = time.time()
#         st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
#         self._log.info('{}:    {}'.format(st, msg))

#     # ...............................................
#     def _getValFromCorrectLine(self, fldname, meta, vline, iline):
#         """
#         @summary: IFF gathering values from separate lines (matched on gbifID)
#                   use metadata to pull the value from the indicated line.
#         @param fldname: field name 
#         @param meta: tuple including datatype and INTERPRETED or VERBATIM 
#                identifier for file to pull value from
#         @param vline: A CSV record of verbatim provider DarwinCore occurrence data 
#         @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
#         """
#         # get column Index in correct file
#         if meta['version'] == VERBATIM:
#             try:
#                 val = vline[self.fldMeta[fldname][VERBATIM]]
#             except KeyError:
#                 print('{} not in VERBATIM data, using INTERPRETED'.format(fldname))
#                 val = iline[self.fldMeta[fldname][INTERPRETED]]
#         else:
#             try:
#                 val = iline[self.fldMeta[fldname][INTERPRETED]]
#             except KeyError:
#                 print('{} not in INTERPRETED data, using VERBATIM'.format(fldname))
#                 try:
#                     val = vline[self.fldMeta[fldname][VERBATIM]]
#                 except Exception:
#                     print('{} not in either file'.format(fldname))
#         return val

    # ...............................................
    def _update_point(self, rec):
        """
        @summary: Update the decimal longitude and latitude, replacing 0,0 with 
                     None, None and ensuring that US points have a negative longitude.
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        @note: record must have lat/lon or countryCode but GBIF query is on countryCode so
               that will never be blank.
        """
        if 'decimalLongitude' not in rec:
            rec['decimalLongitude'] = None

        if 'decimalLatitude' not in rec:
            rec['decimalLatitude'] = None

        if 'countryCode' not in rec:
            rec['countryCode'] = None
            
        # Change 0,0 to None
        if rec['decimalLongitude'] == 0 and rec['decimalLatitude'] == 0:
            rec['decimalLongitude'] = None
            rec['decimalLatitude'] = None
        # Make sure US longitude is negative
        elif (rec['countryCode'] == 'US' 
                and rec['decimalLongitude'] is not None 
                and rec['decimalLongitude'] > 0):
            rec['decimalLongitude'] = rec['decimalLongitude'] * -1 

    # ...............................................
    def _update_locality(self, rec):
        """
        @summary: Update the verbatim_locality, taking in order of preference,
                  verbatimLocality, locality, habitat 
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        """
        if rec['verbatimLocality'] is not None:
            pass
        elif rec['locality'] is not None:
            rec['verbatimLocality'] = rec['locality']
        elif rec['habitat'] is not None:
            rec['verbatimLocality'] = rec['habitat']

    # ...............................................
    def _update_dates(self, rec):
        """
        @summary: Make sure that eventDate is parsable into integers and update 
                     missing year value by extracting from the eventDate.
        @param rec: dictionary of all fieldnames and values for this record
        @note: BISON eventDate should be ISO 8601, ex: 2018-08-01
                 GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        fillyr = False
        # Test year field
        try:
            rec['year'] = int(rec['year'])
        except:
            fillyr = True
            
        # Test eventDate field
        tmp = rec['eventDate']
        if tmp is not None:
            dateonly = tmp.split('T')[0]
            if dateonly != '':
                parts = dateonly.split('-')
                try:
                    for i in range(len(parts)):
                        int(parts[i])
                except:
                    pass
                else:
                    rec['eventDate'] = dateonly
                    if fillyr:
                        rec['year'] = parts[0]

    # ...............................................
    def _save_for_lookups(self, rec):
        """
        @summary: Save scientificName / taxonKey and publisher for parse or query 
        @param rec: dictionary of all fieldnames and values for this record
        @note: The GBIF name parser fails on unicode namestrings
        @note: Invalid records with no scientificName or taxonKey were 
               discarded earlier in self._clean_input
        @note: Names saved to dictionary key=sciname, val=[taxonid, ...] 
               to avoid writing duplicates.  
        """
        # Save a set of publisher UUIDs
        publisher = rec['publisher']
        if publisher is not None:
            self._pubset.add(publisher)
            
        # Save a dict of sciname: taxonKeyList
        sciname = rec['scientificName']
        taxkey = rec['taxonKey']
        try:
            int(taxkey)
        except:
            self._log.warn('gbifID {}: non-integer taxonID {}'.format(
                                rec['gbifID'], taxkey))                
        try:
            keylist = self._namekey[sciname]
            if taxkey not in keylist:
                self._namekey[sciname].append(taxkey)
        except KeyError:
            self._namekey[sciname] = [taxkey]

    # ...............................................
    def _write_lookupvals(self):
        # Write scientific names and taxonKeys found with them in raw data
        self._namekey_writer, self._namef = self._open_for_csv_write(
            self.namekey_fname, header=['scientificName', 'taxonKeys'])
        try:
            for sciname, txkeylst in self._namekey.items():
                row = [k for k in txkeylst]
                row.insert(0, sciname)
                self._namekey_writer.writerow(row)
        finally:
            self._namef.close()
        
        # Write publishers found in raw data
        self._pubf = self._open_for_write(self.pub_fname)
        try:
            for pub in self._pubset:
                self._pubf.write('{}\n'.format(pub))
        finally:
            self._pubf.close()
        
        # Write dataset UUIDs from EML files downloaded with raw data
        dsfnames = glob.glob(os.path.join(self._dataset_pth, '*.xml'))
        if dsfnames is not None:
            self._dskf = self._open_for_write(self.dsk_fname)
            try:
                for fn in dsfnames:
                    self._dskf.write('{}\n'.format(fn[:-4]))
            finally:
                self._dskf.close()


    # ...............................................
    def _test_transform_val(self, fldname, tmpval):
        """
        @summary: Update values with any BISON-requested changed, or signal 
                  to remove the record by returning None for fldname.
        @param fldname: Fieldname in current record
        @param tmpval: Value for this field in current record
        """
        val = tmpval.strip()
        
        # Replace N/A and empty string
        if val.lower() in PROHIBITED_VALS:
            val = None
            
        # remove records with scientificName or taxonKey missing
        elif fldname in ('scientificName', 'taxonKey') and val is None:
            self._log.info('Discarded rec with missing scientificName or taxonKey')
            fldname = val = None

        # remove records with occurrenceStatus = absence
        elif fldname == 'occurrenceStatus' and val.lower() == 'absent':
            self._log.info('Discarded absence record')
            fldname = val = None

        # simplify basisOfRecord terms
        elif fldname == 'basisOfRecord':
            if val in TERM_CONVERT:
                val = TERM_CONVERT[val]
                
        # Convert year to integer
        elif fldname == 'year':
            try:
                val = int(val)
            except:
                val = None
            
        # gather geo fields for check/convert
        elif fldname in ('decimalLongitude', 'decimalLatitude'):
            try:
                val = float(val)
            except Exception:
                val = None
            
        return fldname, val

    # ...............................................
    def _open_for_csv_write(self, fname, header=None):
        '''
        @summary: Read and populate dictionary if file exists, 
                  then re-open for writing or appending. If lookup file 
                  is new, write header if provided.
        '''
        fmode = 'w'        
        if os.path.exists(fname):
            fmode = 'a'
            
        csvwriter, outfile = getCSVWriter(fname, self._bison_delimiter, 
                                          self._encoding, fmode=fmode)
        self._log.info('Opened lookup file {} for write'.format(fname))

        if fmode == 'w' and header is not None:
            csvwriter.writerow(header)
        
        return csvwriter, outfile 
    
    
    # ...............................................
    def _open_for_write(self, fname):
        '''
        @summary: Open for writing or appending. 
        '''
        fmode = 'w'        
        if os.path.exists(fname):
            fmode = 'a'
        outf = open(fname, mode=fmode, encoding=self._encoding)
        self._log.info('Opened file {}'.format(fname))

        return outf
    
    # ...............................................
    def _read_dict(self, fname):
        '''
        @summary: Read and populate dictionary if file exists
        '''
        lookupDict = {}
        if os.path.exists(fname):
            recno = 0        
            try:
                csvRdr, infile = getCSVReader(fname, self._bison_delimiter, 
                                              self._encoding)
                # get header
                line, recno = getLine(csvRdr, recno)
                # read lookup vals into dictionary
                while (line is not None):
                    line, recno = getLine(csvRdr, recno)
                    if line and len(line) > 0:
                        try:
                            # First item is dict key, rest are vals
                            lookupDict[line[0]] = line[1:]
                        except Exception:
                            self._log.warn('Failed to read line {} from {}'
                                                .format(recno, fname))
                self._log.info('Read lookup file {}'.format(fname))
            except Exception as e:
                self._log.error('Failed reading data in line {} of {}: {}'
                                .format(fname, recno, e))
            finally:
                infile.close()
        return lookupDict
        
    # ...............................................
    def _read_list(self, fname):
        '''
        @summary: Read and populate list if file exists. 
        '''
        lookupvals = set()
        
        if os.path.exists(fname):
            num = 0        
            try:
                inf = open(fname, mode='r', encoding=self._encoding)
                for line in inf:
                    num +=1
                    val = line.strip()
                    lookupvals.add(val)
                self._log.info('Read lookup file {}'.format(fname))
            except Exception as e:
                self._log.error('Exception reading data in line {} of {}: {}'
                                .format(num, fname, e))
            finally:
                inf.close()
        
        return lookupvals
    
    # ...............................................
    def open_read_files(self):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                  output file for writing
        '''
        # Extract relevant GBIF metadata
        self.fldMeta = self.get_field_meta()

        # Open raw GBIF data
        (self._reader, 
         self._inf) = getCSVReader(self.interp_fname, self._gbif_delimiter, 
                                   self._encoding)
        self._files.append(self._inf) 
        
        # Open output BISON file 
        (self._writer, 
         self._outf) = getCSVWriter(self.outfname, self._bison_delimiter, 
                                   self._encoding)
        self._files.append(self._outf)

        # Read any existing values for lookup
        self._namekey = self._read_dict(self.namekey_fname)
        self._pubset = self._read_list(self.pub_fname)
        
        # Write the header row 
        self._writer.writerow(ORDERED_OUT_FIELDS)
        self._log.info('Opened input/output files, read lookup values')

    # ...............................................
    def is_open(self):
        """
        @summary: Return true if any files are open.
        """
        for f in self._files:
            if not f is None and not f.closed:
                return True
        return False

    # ...............................................
    def close(self):
        '''
        @summary: Close input datafiles and output file
        '''
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass

    # ...............................................
    def get_field_meta(self):
        '''
        @todo: Remove interpreted / verbatim file designations, interpreted cannot 
                 be joined to verbatim file for additional info.
        @summary: Read metadata for interpreted data file, and
                     for fields we are interested in:
                     extract column index for each file, add datatype. 
                     Resulting metadata will look like:
                            fields = {term: (columnIndex, dtype), 
                                         ...
                                         }
        '''
        tdwg = '{http://rs.tdwg.org/dwc/text/}'
        fields = {}
        tree = ET.parse(self._meta_fname)
        root = tree.getroot()
        # Child will reference INTERPRETED or VERBATIM file
        for child in root:
            # does this node of metadata reference INTERPRETED or VERBATIM?
            fileElt = child.find('tdwg:files', NAMESPACE)
            fnameElt= fileElt .find('tdwg:location', NAMESPACE)
            meta4data = fnameElt.text

            if meta4data.startswith(INTERPRETED):
                flds = child.findall(tdwg+'field')
                for fld in flds:
                    # Get column num and short name
                    idx = int(fld.get('index'))
                    temp = fld.get('term')
                    term = temp[temp.rfind(CLIP_CHAR)+1:]
                    # Save only fields of interest
                    if term in SAVE_FIELDS:
                        dtype = SAVE_FIELDS[term][0]
                        if not term in fields:
                            fields[term] = (idx, dtype)
        return fields

    # ...............................................
    def _update_rec(self, rec):
        """
        @summary: Update record with all BISON-requested changes, or remove 
                     the record by setting it to None.
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        """
        if rec is not None:
            # Fill verbatimLocality with anything available
            self._update_locality(rec)
            # Format eventDate and fill missing year
            self._update_dates(rec)
            # Modify lat/lon vals if necessary
            self._update_point(rec)
            # Save scientificName / TaxonID, providerID and datasetKey for later lookup and replace
            self._save_for_lookups(rec)

    # ...............................................
    def _clean_input(self, iline):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        gbifID = iline[0]
        rec = {'gbifID': gbifID}
        # Find values for gbif fields of interest
        for fldname, (idx, _) in self.fldMeta.items():
            try:
                tmpval = iline[idx]
            except Exception:
                self._log.warning('Failed to get column {}/{} for gbifID {}'
                                  .format(idx, fldname, gbifID))
                val = None
            else:
                # Invalid records will return None for fldname
                fldname, val = self._test_transform_val(fldname, tmpval)
                
                if fldname is None:
                    self._log.info('Invalid record with gbifID {} discarded'
                                   .format(gbifID))
                    break
                    rec = None
                
                else:
                    rec[fldname] = val
                            
        return rec

    # ...............................................
    def create_bisonrec_step1(self, iline):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        row = []
        rec = self._clean_input(iline)
        
        if rec is not None:
            # Update values 
            self._update_rec(rec)
            # create the ordered row
            for fld in ORDERED_OUT_FIELDS:
                try:
                    row.append(rec[fld])
                except KeyError:
                    # These 2 fields filled in on 2nd pass
                    if fld not in COMPUTE_FIELDS:
                        print ('Missing field {} in record with gbifID {}'
                                 .format(fld, rec['gbifID']))
                    row.append('')
        return row

    # ...............................................
    def first_pass(self):
        """
        @summary: Create a CSV file containing GBIF occurrence records extracted 
                     from the interpreted occurrence file provided 
                     from an Occurrence Download, in Darwin Core format.  Values may
                     be modified and records may be discarded according to 
                     BISON requests.
        @return: A CSV file of first pass of BISON-modified records from a 
                 GBIF download. 
        @note: Some fields will be filled in on subsequent processing.
        """
        if self.is_open():
            self.close()
        if os.path.exists(self.outfname):
            raise Exception('Bison output file {} exists!'.format(self.outfname))
        
        recno = 0
        try:
            self.open_read_files()
            
            # Pull the header row 
            header, recno = getLine(self._reader, recno)
            line = header
            while (line is not None):
                
                # Get interpreted record
                line, recno = getLine(self._reader, recno)
                if line is None:
                    break
                
                # Create new record
                byline = self.create_bisonrec_step1(line)
                
                # Write new record
                if byline:
                    self._writer.writerow(byline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self._write_lookupvals()
            
    # ...............................................
    def create_lookup_tables(self):
        """
        @summary: Create lookup tables for: 
                  BISON providerID from GBIF publisher
                  BISON resourceID from GBIF datasetKey
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        @return: Three files, one for each lookup table. 
        """
        self.gbifapi = GbifAPI()
        
        self._resolve_names()
        
        # aka publisher
        self._resolve_providers()
        # aka dataset
        self._resolve_resources()
        
        # Write scientific names and taxonKeys found with them in raw data
        self._namekey_writer, self._namef = self._open_for_csv_write(
            self.namekey_fname, header=['scientificName', 'taxonKeys'])
        try:
            for sciname, txkeylst in self._namekey.items():
                row = [k for k in txkeylst]
                row.insert(0, sciname)
                self._namekey_writer.writerow(row)
        finally:
            self._namef.close()
        
        # Write publishers found in raw data
        self._pubf = self._open_for_write(self.pub_fname)
        try:
            for pub in self._pubset:
                self._pubf.write('{}\n'.format(pub))
        finally:
            self._pubf.close()
        
        # Write dataset UUIDs from EML files downloaded with raw data
        dsfnames = glob.glob(os.path.join(self._dataset_pth, '*.xml'))
        if dsfnames is not None:
            self._dskf = self._open_for_write(self.dsk_fname)
            try:
                for fn in dsfnames:
                    self._dskf.write('{}\n'.format(fn[:-4]))
            finally:
                self._dskf.close()

# ...............................................
if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser(
#                 description=("""Parse a GBIF occurrence dataset downloaded
#                                      from the GBIF occurrence web service in
#                                      Darwin Core format into BISON format.  
#                                  """))
#     parser.add_argument('base_path', type=str, 
#                         help='Absolute pathname of the directory for data transform')
#     parser.add_argument('gbif_file', type=str, 
#                         help='Relative filename of the input GBIF occurrence file')
#     parser.add_argument('bison_file', type=str, 
#                         help='Relative filename of the output BISON occurrence data.')
#     parser.add_argument('--names_only', type=bool, default=False,
#                 help=('Re-read a bison output file to retrieve scientificName and taxonID.'))
#     args = parser.parse_args()
#     rereadNames = args.names_only
#     gbif_relfname = args.gbif_file
#     bison_relfname = args.bison_file

    # Testing data
    rereadNames = False
    basepath = '/tank/data/bison/2019'
    gbif_relfname = 'raw/occurrence_10.txt'
    bison_relfname = 'tmp/step1.csv'
    overwrite = True
    
    gbif_fname = os.path.join(basepath, gbif_relfname)
    bison_fname = os.path.join(basepath, bison_relfname)
    rawpth, _ = os.path.split(gbif_fname)
    meta_fname = os.path.join(rawpth, META_FNAME)
        
    if os.path.exists(bison_fname):
        if overwrite:
            print('Removing old output file {}'.format(bison_fname))
            os.remove(bison_fname)
        else:
            raise Exception('Output file {} already exists'.format(bison_fname))
            
    if os.path.exists(gbif_fname):        
        gr = GBIFReader(gbif_fname, meta_fname, bison_fname)
        print('Calling program with input/output {}'.format(gbif_fname, bison_fname))
        gr.first_pass()
    else:
        raise Exception('Filename {} does not exist'.format(gbif_fname))
    
    
"""
wc -l occurrence.txt 
71057978 occurrence.txt


import datetime
import os
import time
import xml.etree.ElementTree as ET
from gbif.constants import (IN_DELIMITER, OUT_DELIMITER, PROHIBITED_VALS, 
                            VERBATIM, INTERPRETED, TERM_CONVERT, ENCODING,
                            ORDERED_OUT_FIELDS, NAMESPACE, SAVE_FIELDS,
                            CLIP_CHAR, META_FNAME)

from gbif.gbif2bison import *


rereadNames = False
basepath = '/tank/data/bison/2019'
gbif_relfname = 'raw/occurrence.txt'
bison_relfname = 'tmp/bison_pass1.csv'
overwrite = True

gbif_fname = os.path.join(basepath, gbif_relfname)
bison_fname = os.path.join(basepath, bison_relfname)
rawpth, _ = os.path.split(gbif_fname)
meta_fname = os.path.join(rawpth, META_FNAME)
    
if os.path.exists(bison_fname):
    if overwrite:
        print('Removing old output file {}'.format(bison_fname))
        os.remove(bison_fname)
    else:
        raise Exception('Output file {} already exists'.format(bison_fname))

self = GBIFReader(gbif_fname, meta_fname, bison_fname)

recno = 0
self._namekey, self._namekey_writer, self._namekeyf = \
        self._read_open_for_write(self.namekey_fname,
                               header=['scientificName', 'taxonKey'])
self.openInputOutput()
line, recno = getLine(self._reader, recno)
# while (line is not None):

line, recno = getLine(self._reader, recno)
if line is None:
    print('line is None')

byline = self.createBisonLine(line)
if byline:
    self._writer.writerow(byline)

"""
