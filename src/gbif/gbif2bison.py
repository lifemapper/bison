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
                            CLIP_CHAR, META_FNAME, NAMESPACE, 
                            GBIF_DSET_KEYS, GBIF_ORG_KEYS)
from gbif.lookup import Looker
from gbif.tools import (getCSVReader, getCSVDictReader, getCSVWriter, getLine, 
                        getLogger)
from gbif.gbifapi import GbifAPI

        
# .............................................................................
class GBIFReader(object):
    """
    @summary: GBIF Record containing CSV record of 
                 * original provider data from verbatim.txt
                 * GBIF-interpreted data from occurrence.txt
    @note: To chunk the file into more easily managed small files (i.e. fewer 
             GBIF API queries), split using sed command output to file like: 
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-5000 are lines to delete, and 10000 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, basepath, indir, tmpdir, outdir):
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
        
        self._log = None
        self._files = []
        self.inpath = os.path.join(basepath, indir)
        self.outpath = os.path.join(basepath, outdir)
        self.tmppath = os.path.join(basepath, tmpdir)
        self._dataset_pth = os.path.join(self.inpath, 'dataset')
        
        # ....................
        self.interp_fname = None
        self._inf = None
        self._reader = None

        self._meta_fname = None
        self.fldMeta = None

        self.outfname = None
        self._outf = None
        self._writer = None
                
        # Canonical lookup tmp data
        self.namekey_fname = None
        self._namef = None
        self._namekey_writer = None
        self._namekey = {}
        # Input for Publisher lookup (providerID)
        self.pub_fname = None
        self._pubf = None
        self._pubset = set()
        
        # Lookup table output files
        self.dset_lut_fname = os.path.join(self.tmppath, 'dataset_lookup.csv')
        self.dset_org_lut_fname = os.path.join(self.tmppath, 'dataset_org_lookup.csv')
        self.name_lut_fname = os.path.join(self.tmppath, 'name_lookup.csv')
        
    # ...............................................
    def _set_pass1_vars(self, interpreted_fname, meta_fname, out_fname):
        
        self.interp_fname = os.path.join(self.inpath, interpreted_fname)
        self._meta_fname = os.path.join(self.inpath, meta_fname)
        self.outfname = os.path.join(self.tmppath, out_fname)
        # Lookup tmp data
        self.namekey_fname = os.path.join(self.tmppath, 'namekey_list.csv')
        self.pub_fname = os.path.join(self.tmppath, 'publisher_list.txt')

    # ...............................................


#     # ...............................................
#     def _getValFromCorrectLine(self, fld, meta, vline, iline):
#         """
#         @summary: IFF gathering values from separate lines (matched on gbifID)
#                   use metadata to pull the value from the indicated line.
#         @param fld: field name 
#         @param meta: tuple including datatype and INTERPRETED or VERBATIM 
#                identifier for file to pull value from
#         @param vline: A CSV record of verbatim provider DarwinCore occurrence data 
#         @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
#         """
#         # get column Index in correct file
#         if meta['version'] == VERBATIM:
#             try:
#                 val = vline[self.fldMeta[fld][VERBATIM]]
#             except KeyError:
#                 print('{} not in VERBATIM data, using INTERPRETED'.format(fld))
#                 val = iline[self.fldMeta[fld][INTERPRETED]]
#         else:
#             try:
#                 val = iline[self.fldMeta[fld][INTERPRETED]]
#             except KeyError:
#                 print('{} not in INTERPRETED data, using VERBATIM'.format(fld))
#                 try:
#                     val = vline[self.fldMeta[fld][VERBATIM]]
#                 except Exception:
#                     print('{} not in either file'.format(fld))
#         return val

    # ...............................................
    def _rotate_logfile(self, logname=None):
        if logname is None:
            nm, _ = os.path.splitext(os.path.basename(__file__))
            logname = '{}.{}'.format(nm, int(time.time()))
        logfname = os.path.join(self.outpath, '{}.log'.format(logname))
        if self._log is not None:
            self._log = None
        self._log = getLogger(logname, logfname)
        
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
        if rec is not None:
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
        return rec

    # ...............................................
    def _update_locality(self, rec):
        """
        @summary: Update the verbatim_locality, taking in order of preference,
                  verbatimLocality, locality, habitat 
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        """
        if rec is not None:
            gid = rec['gbifID']
            locality = None
            try:
                locality = rec['verbatimLocality'] 
            except:
                self._log.info('Rec gbifID {} missing verbatimLocality field'
                               .format(gid))
    
            if locality is None:
                try:
                    locality = rec['locality'] 
                except:
                    self._log.info('Rec gbifID {} missing locality field'
                                   .format(gid))
                    
            if locality is None:
                try:
                    locality = rec['habitat']
                except:
                    self._log.info('Rec gbifID {} missing habitat field'
                                   .format(gid))
        return rec

    # ...............................................
    def _update_dates(self, rec):
        """
        @summary: Make sure that eventDate is parsable into integers and update 
                     missing year value by extracting from the eventDate.
        @param rec: dictionary of all fieldnames and values for this record
        @note: BISON eventDate should be ISO 8601, ex: 2018-08-01
                 GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        if rec is not None:
            gid = rec['gbifID']
            fillyr = False
            # Test year field
            try:
                rec['year'] = int(rec['year'])
            except:
                fillyr = True
                
            # Test eventDate field
            try:
                tmp = rec['eventDate']
            except:
                self._log.info('Rec gbifID {} missing eventDate field'
                               .format(gid))
            else:
                if tmp is not None:
                    dateonly = tmp.split('T')[0]
                    if dateonly != '':
                        parts = dateonly.split('-')
                        try:
                            for i in range(len(parts)):
                                int(parts[i])
                        except:
                            self._log.info('Rec gbifID {} has invalid eventDate {}'
                                              .format(gid, rec['eventDate']))
                            pass
                        else:
                            rec['eventDate'] = dateonly
                            if fillyr:
                                rec['year'] = parts[0]
        return rec

    # ...............................................
    def _save_for_lookups(self, rec):
        """
        @summary: Save scientificName / taxonKey and publisher for parse or query 
        @param rec: dictionary of all fieldnames and values for this record
        @note: The GBIF name parser fails on unicode namestrings
        @note: Invalid records with no scientificName or taxonKey were 
               discarded earlier in self._clean_input_values
        @note: Names saved to dictionary key=sciname, val=[taxonid, ...] 
               to avoid writing duplicates.  
        """
        if rec is not None:
            gid = rec['gbifID']
            # Save a set of publisher UUIDs
            try:
                publisher = rec['publisher']
            except:
                self._log.info('Rec gbifID {} missing publisher field'
                               .format(gid))
            else:
                if publisher is not None:
                    self._pubset.add(publisher)
                
            # Previously tested for existence of these fields
            # Save a dict of sciname: taxonKeyList
            try:
                sciname = rec['scientificName']
                taxkey = rec['taxonKey']
            except:
                raise 
            else:
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
        # TODO: Remove?  No publisher fields found yet
        if len(self._pubset) == 0:
            self._log.info('No publisher data to save')
        else:
            self._pubf = self._open_for_write(self.pub_fname)
            try:
                for pub in self._pubset:
                    self._pubf.write('{}\n'.format(pub))
            finally:
                self._pubf.close()


    # ...............................................
    def _test_transform_val(self, fld, tmpval):
        """
        @summary: Update values with any BISON-requested changed, or signal 
                  to remove the record by returning None for fld.
        @param fld: Fieldname in current record
        @param tmpval: Value for this field in current record
        """
        do_discard = False
        val = tmpval.strip()
        
        # Replace N/A and empty string
        if val.lower() in PROHIBITED_VALS:
            val = None
            
        # remove records with scientificName missing
        elif fld == 'scientificName' and val is None:
            do_discard = True
            
        # remove records with taxonKey missing
        elif fld == 'taxonKey': 
            if val is None:
                do_discard = True
            else:
                try:
                    val = int(val)
                except:
                    do_discard = True

        # remove records with occurrenceStatus = absence
        elif fld == 'occurrenceStatus' and val.lower() == 'absent':
            do_discard = True

        # simplify basisOfRecord terms
        elif fld == 'basisOfRecord':
            if val in TERM_CONVERT:
                val = TERM_CONVERT[val]
                
        # Convert year to integer
        elif fld == 'year':
            try:
                val = int(val)
            except:
                self._log.info('    Remove invalid year field {}'.format(val))
                val = None
            
        # gather geo fields for check/convert
        elif fld in ('decimalLongitude', 'decimalLatitude'):
            try:
                val = float(val)
            except Exception:
                self._log.info('    Remove invalid {} field {}'.format(fld, val))
                val = None
            
        return do_discard, val

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
    def _read_name_keys(self, fname):
        '''
        @summary: Read and populate dictionary with key = name and 
                  value = one or more taxonids single or list of taxonif file exists
        '''
        lookupDict = {}
        if os.path.exists(fname):
            recno = 0        
            try:
                csvRdr, infile = getCSVReader(fname, self._bison_delimiter, 
                                              self._encoding)
                # get header
                self._log.info('Read lookup file {} ...'.format(fname))
                line, recno = getLine(csvRdr, recno)
                # read lookup vals into dictionary
                while (line is not None):
                    line, recno = getLine(csvRdr, recno)
                    if line and len(line) > 0:
                        try:
                            # First item is dict key, rest are vals
                            lookupDict[line[0]] = list(line[1:])
                        except Exception:
                            self._log.warn('Failed to read line {} from {}'
                                                .format(recno, fname))
            except Exception as e:
                self._log.error('Failed reading data in line {} of {}: {}'
                                .format(fname, recno, e))
            finally:
                infile.close()
        return lookupDict
        
#     # ...............................................
#     def _read_list(self, fname):
#         '''
#         @summary: Read and populate list if file exists. 
#         '''
#         lookupvals = set()
#         
#         if os.path.exists(fname):
#             num = 0        
#             try:
#                 inf = open(fname, mode='r', encoding=self._encoding)
#                 for line in inf:
#                     num +=1
#                     val = line.strip()
#                     lookupvals.add(val)
#                 self._log.info('Read lookup file {}'.format(fname))
#             except Exception as e:
#                 self._log.error('Exception reading data in line {} of {}: {}'
#                                 .format(num, fname, e))
#             finally:
#                 inf.close()
#         
#         return lookupvals
    
    # ...............................................
    def _open_pass1_files(self):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                  output file for writing
        '''
        # Extract relevant GBIF metadata
        self._log.info('Read metadata ...')
        self.fldMeta = self.get_field_meta()

        # Open raw GBIF data
        self._log.info('Open raw GBIF input file {}'.format(self.interp_fname))
        (self._reader, 
         self._inf) = getCSVReader(self.interp_fname, self._gbif_delimiter, 
                                   self._encoding)
        self._files.append(self._inf) 
        
        # Open output BISON file 
        self._log.info('Open step1 BISON output file {}'.format(self.outfname))
        (self._writer, 
         self._outf) = getCSVWriter(self.outfname, self._bison_delimiter, 
                                   self._encoding)
        self._files.append(self._outf)
        self._writer.writerow(ORDERED_OUT_FIELDS)

        # Read any existing values for lookup
        namekeys = self._read_name_keys(self.namekey_fname)
            
#         if os.path.exists(self.pub_fname):
#             self._log.info('Read existing publishers from {}'.format(self.pub_fname))
#             lookupvals = set()
#             num = 0        
#             try:
#                 inf = open(self.pub_fname, mode='r', encoding=self._encoding)
#                 for line in inf:
#                     num +=1
#                     val = line.strip()
#                     lookupvals.add(val)
#             except Exception as e:
#                 self._log.error('Exception reading data in line {} of {}: {}'
#                                 .format(num, self.pub_fname, e))
#             finally:
#                 inf.close()
            
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
                        if not term in fields:
                            fields[term] = idx
                        else:
                            self._log.info('Duplicate field {}, idxs {} and {}'
                                           .format(term, fields[term], idx))
#                     # Save all fields
#                     if not term in fields:
#                         fields[term] = idx
#                     else:
#                         self._log.info('Duplicate field {}, idxs {} and {}'
#                                        .format(term, fields[term], idx))
        return fields

#     # ...............................................
#     def _update_rec(self, rec):
#         """
#         @summary: Update record with all BISON-requested changes, or remove 
#                      the record by setting it to None.
#         @param rec: dictionary of all fieldnames and values for this record
#         @note: function modifies original dict
#         """
#         if rec is not None:
#             # Fill verbatimLocality with anything available
#             self._update_locality(rec)
#             # Format eventDate and fill missing year
#             self._update_dates(rec)
#             # Modify lat/lon vals if necessary
#             self._update_point(rec)
#             # Save scientificName / TaxonID, providerID and datasetKey for later lookup and replace
#             self._save_for_lookups(rec)

    # ...............................................
    def _test_required_fields(self, rec):
        """
        @summary: Update values with any BISON-requested changed, or signal 
                  to remove the record by returning None for fld.
        @param rec: current record dictionary
        """
        if rec is not None:
            gid = rec['gbifID']
            # Missing fields could mean mis-aligned data
            for fld in self.fldMeta.keys():
                if not fld in rec:
                    self._log.warning('Data misalignment? Missing {} in rec gbifID {}'
                                      .format(fld, gid))
    
            # Required fields exist 
            sciname = taxkey = None
            try:
                sciname = rec['scientificName']
            except:
                rec = None
                self._log.info('Discarded rec with missing scientificName field')
            else:
                try:
                    taxkey = rec['taxonKey']
                except:
                    rec = None
                    self._log.info('Discarded rec with missing taxonKey field')
                
            # Required fields have values
            if rec and (taxkey is None and sciname is None):
                rec = None
                self._log.info('Discarded rec gbifID {} missing both sciname and taxkey'
                               .format(gid))
        return rec

    # ...............................................
    def _clean_input_values(self, iline):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        gid = iline[0]
        rec = {'gbifID': gid}
        # Find values for gbif fields of interest
        for fld, idx in self.fldMeta.items():
            # Check column existence
            try:
                tmpval = iline[idx]
            except Exception:
                self._log.warning('Failed to get column {}/{} in rec gbifID {}'
                                  .format(idx, fld, gid))
                val = None
            else:
                # Test each field/value, invalid records will return fld = None
                do_discard, val = self._test_transform_val(fld, tmpval)
                
                if do_discard is True:
                    self._log.info('Discard invalid rec gbifID {} with {} = {}'
                                   .format(gid, fld, val))
                    rec = None
                    break                
                else:
                    rec[fld] = val

        return rec

    # ...............................................
    def _create_bisonrec_step1(self, iline):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        biline = []
        rec = self._clean_input_values(iline)
        
        # Check full record
        rec = self._test_required_fields(rec)
        # Fill verbatimLocality with anything available
        rec = self._update_locality(rec)
        # Format eventDate and fill missing year
        rec = self._update_dates(rec)
        # Modify lat/lon vals if necessary
        rec = self._update_point(rec)
        # Save scientificName / TaxonID and providerID for later lookup and replace
        self._save_for_lookups(rec)
        
        if rec is not None:
            # create the ordered row
            for fld in ORDERED_OUT_FIELDS:
                try:
                    biline.append(rec[fld])
                except KeyError:
                    biline.append('')
                    # Fields filled in on 2nd pass
                    if fld not in COMPUTE_FIELDS:
                        print ('Missing field {} in record with gbifID {}'
                                 .format(fld, rec['gbifID']))
        return biline

    # ...............................................
    def first_pass(self, gbif_fname, meta_fname, bison_fname, logname=None):
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
        self._rotate_logfile(logname=logname)
        self._set_pass1_vars(gbif_fname, meta_fname, bison_fname)

        loginterval = 1000000
        recno = 0
        try:
            self._open_pass1_files()
            
            # Pull the header row 
            header, recno = getLine(self._reader, recno)
            line = header
            while (line is not None):
                
                # Get interpreted record
                line, recno = getLine(self._reader, recno)
                if line is None:
                    break
                elif (recno % loginterval) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                    
                # Create new record or empty list
                biline = self._create_bisonrec_step1(line)
                
                # Write new record
                if biline:
                    self._writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self._write_lookupvals()
            
    # ...............................................
    def second_pass(self, logname=None):
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
        self._rotate_logfile(logname=logname)

        loginterval = 1000000
        recno = 0
        try:
            self._open_pass1_files()
            
            # Pull the header row 
            header, recno = getLine(self._reader, recno)
            line = header
            while (line is not None):
                
                # Get interpreted record
                line, recno = getLine(self._reader, recno)
                if line is None:
                    break
                elif (recno % loginterval) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                    
                # Create new record or empty list
                biline = self.create_bisonrec_step1(line)
                
                # Write new record
                if biline:
                    self._writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self._write_lookupvals()
            
    # ...............................................
    def create_lookup_tables(self, logname=None):
        """
        @summary: Create lookup tables for: 
                  BISON providerID from GBIF publisher
                  BISON resourceID from GBIF datasetKey
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        @return: Three files, one for each lookup table. 
        """
        self._rotate_logfile(logname=logname)
        
        self.write_dataset_org_lookup(log=self._log)
        self.write_name_lookup(log=self._log)
                    
    # ...............................................
    def write_dataset_org_lookup(self, log=None, logname=None):
        """
        @summary: Create lookup table for: 
                  BISON resource and provider from 
                  GBIF datasetKey and dataset publishingOrganizationKey
        @return: One file, containing dataset metadata, 
                           including publishingOrganization metadata 
                           for that dataset
        """
        if log is None:
            self._rotate_logfile(logname=logname)
        
        gbifapi = GbifAPI()
        # Gather dataset UUIDs from EML files downloaded with raw data
        uuids = []
        dsfnames = glob.glob(os.path.join(self._dataset_pth, '*.xml'))
        if dsfnames is not None:
            for fn in dsfnames:
                uuids.append(fn[:-4])
        # Query/save dataset information
        gbifapi.get_write_dataset_meta(self.dset_lut_fname, uuids)
        # Query organization info for each dataset and save
        gbifapi.rewrite_dataset_with_orgs(self.dset_lut_fname, 
                                          self.dset_org_lut_fname)
        return self.dset_org_lut_fname

    # ...............................................
    def write_name_lookup(self, log=None, logname=None):
        """
        @summary: Create lookup tables for: 
                  BISON providerID from GBIF publisher
                  BISON resourceID from GBIF datasetKey
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        @return: Three files, one for each lookup table. 
        """
        if log is None:
            self._rotate_logfile(logname=logname)
        
        gbifapi = GbifAPI()

        # Read name/taxonIDs dictionary for name resolution
        namekeys = self._read_name_keys(self.namekey_fname)
        # Write names to send to parser
        parser_in = os.path.join(self.outpath, 'parser_in.txt')
        if os.path.exists(parser_in):
            os.remove(parser_in)
        with open(parser_in, 'w', encoding=ENCODING) as f:
            for name in namekeys.keys():
                f.write('{}\n'.format(name))
        
        name_fail = gbifapi.get_write_parsednames(parser_in, self.name_lut_fname)
        with open(self.name_lut_fname, 'a', encoding=ENCODING) as f:            
            for badname in name_fail:
                taxonkeys = namekeys[badname]
                for tk in taxonkeys:
                    canonical = gbifapi.find_canonical(taxkey=tk)
                    f.write('{}\n'.format(canonical))
        
        return self.name_lut_fname
            
        
# ...............................................
if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser(
#                 description=("""Parse a GBIF occurrence dataset downloaded
#                                      from the GBIF occurrence web service in
#                                      Darwin Core format into BISON format.  
#                                  """))
#     parser.add_argument('base_path', type=str, 
#                         help="""
#                         Absolute pathname of the directory for data transform.
#                         Path should contain 'raw' subdirectory with downloaded
#                         GBIF data and metadata.  Subdirectories 'tmp' and 'out' 
#                         will be created if necessary, and used for temporary 
#                         and final output files.
#                         """)
#     parser.add_argument('gbif_file', type=str, 
#                         help='Base filename of the input GBIF occurrence file')
#     parser.add_argument('bison_file', type=str, 
#                         help='Base filename of the output BISON occurrence data.')
#     args = parser.parse_args()
#     rereadNames = args.names_only
#     gbif_relfname = args.gbif_file
#     bison_relfname = args.bison_file

    # Testing data
    rereadNames = False
    basepath = '/tank/data/bison/tst'
#     basepath = '/tank/data/bison/2019'
    indir = 'raw'
    tmpdir = 'tmp'
    outdir = 'out'
    gbif_fname = 'occurrence_lines_1-3000000.txt'
#     gbif_fname = 'occurrence.txt'
    bison_fname = 'step1.csv'
    overwrite = True
    
    inpath = os.path.join(basepath, indir)
    tmppath = os.path.join(basepath, tmpdir)
    outpath = os.path.join(basepath, outdir)
    
#     gbif_fname = os.path.join(inpath, gbif_fname)
#     bison_fname = os.path.join(tmppath, bison_fname)
#     meta_fname = os.path.join(inpath, META_FNAME)
        
    if os.path.exists(bison_fname):
        if overwrite:
            print('Removing old output file {}'.format(bison_fname))
            os.remove(bison_fname)
        else:
            raise Exception('Output file {} already exists'.format(bison_fname))
        
    os.makedirs(tmppath, mode=0o775, exist_ok=True)
    os.makedirs(outpath, mode=0o775, exist_ok=True)
            
    if os.path.exists(gbif_fname):
        logname = 'create_lut'
        gr = GBIFReader(basepath, indir, tmpdir, outdir)
        print('Calling program with input/output {}'.format(gbif_fname, bison_fname))
#         gr.first_pass(gbif_fname, META_FNAME, bison_fname, overwrite=True)
        gr.create_lookup_tables()
    else:
        raise Exception('Filename {} does not exist'.format(gbif_fname))
    
    
"""
wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv



import os
import time
import xml.etree.ElementTree as ET
from gbif.constants import (IN_DELIMITER, OUT_DELIMITER, PROHIBITED_VALS, 
                            VERBATIM, INTERPRETED, TERM_CONVERT, ENCODING,
                            ORDERED_OUT_FIELDS, NAMESPACE, SAVE_FIELDS,
                            CLIP_CHAR, META_FNAME)

from gbif.gbif2bison import *


# Testing data
basepath = '/tank/data/bison/tst'
indir = 'raw'
tmpdir = 'tmp'
outdir = 'out'
gbif_fname = 'occurrence_lines_1572200-1572250.txt'
bison_fname = 'step1.csv'
overwrite = True

inpath = os.path.join(basepath, indir)
tmppath = os.path.join(basepath, tmpdir)
outpath = os.path.join(basepath, outdir)

gbif_fname = os.path.join(inpath, gbif_fname)
bison_fname = os.path.join(tmppath, bison_fname)
meta_fname = os.path.join(inpath, META_FNAME)
    
if os.path.exists(bison_fname):
    if overwrite:
        print('Removing old output file {}'.format(bison_fname))
        os.remove(bison_fname)
    else:
        raise Exception('Output file {} already exists'.format(bison_fname))
    
os.makedirs(tmppath, mode=0o775, exist_ok=True)
os.makedirs(outpath, mode=0o775, exist_ok=True)
            
self = GBIFReader(gbif_fname, meta_fname, bison_fname)

fields = self.get_field_meta()
"""
