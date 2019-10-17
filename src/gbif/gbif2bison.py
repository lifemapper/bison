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
                            GBIF_UUID_KEY, GBIF_ORG_UUID_FOREIGN_KEY)
from gbif.tools import (getCSVReader, getCSVDictReader, 
                        getCSVWriter, getCSVDictWriter, getLine, getLogger)
from gbif.gbifapi import GbifAPI
from pympler import asizeof
from pickle import NONE

# Rough log of processing progress
LOGINTERVAL = 1000000
        
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
    def __init__(self, basepath, tmpdir, outdir):
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
        self.inpath = basepath
        self.outpath = os.path.join(basepath, outdir)
        self.tmppath = os.path.join(basepath, tmpdir)
        self._dataset_pth = os.path.join(self.inpath, 'dataset')
        
        # ....................
        self.interp_fname = None

        self._meta_fname = None
        self.fldMeta = None

        # Canonical lookup tmp data
        self.nametaxa_fname = None
        self._nametaxa = {}
        
        # Lookup tmp data
        self.nametaxa_fname = os.path.join(self.tmppath, 'sciname_taxkey_list.csv')

        # Lookup table output files
        self.dset_lut_fname = os.path.join(self.tmppath, 'dataset_lookup.csv')
        self._ds_lut = {}
        self.org_lut_fname = os.path.join(self.tmppath, 'org_lookup.csv')
        self._org_lut = {}
        self.name_lut_fname = os.path.join(self.tmppath, 'name_lookup.csv')
        self._name_lut = {}
        
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
        if self._log is None:
            if logname is None:
                nm, _ = os.path.splitext(os.path.basename(__file__))
                logname = '{}.{}'.format(nm, int(time.time()))
            logfname = os.path.join(self.tmppath, '{}.log'.format(logname))
            self._log = getLogger(logname, logfname)

    # ...............................................
    def _add_resource_values(self, rec):
        """
        @summary: Update the resource values from dataset key and metadata LUT
        @note: function modifies original dict
        """
        if rec is not None:
            # GBIF datasetKey field, title, homepage
            dskey = None
            title = None
            url = None
            orgkey = None            
            try:
                dskey = rec['datasetKey']
            except:
                rec['datasetKey'] = None
            else:
                try:
                    metavals = self._ds_lut[dskey]
                except:
                    self._log.warning('{} missing from dataset LUT'.format(dskey))
                else:
                    title = metavals['title']
                    url = metavals['homepage']
                    orgkey = metavals['publishingOrganizationKey']
                    if url.find('bison.org'):
                        rec = None
                    else:    
                        rec['resourceID'] = dskey
                        rec['publishingOrganizationKey'] = orgkey
                        rec['ownerInstitutionCode'] = title
                        rec['collectionID'] = url
            
        return rec

    # ...............................................
    def _add_provider_values(self, rec):
        """
        @summary: Update the provider values from publishingOrganizationKey key 
                  in dataset metadata, and LUT from organization metadata
        @note: function modifies original dict
        """
        if rec is not None:
            # GBIF publishingOrganizationKey (from dataset LUT)
            orgkey = None            
            title = None
            url = None
            
            try:
                orgkey = rec['publishingOrganizationKey']
            except: 
                rec['publishingOrganizationKey'] = None
            else:
                try:
                    metavals = self._org_lut[orgkey]
                except:
                    self._log.warning('{} missing from organization LUT'.format(orgkey))
                else:
                    title = metavals['title']
                    url = metavals['homepage']
                    if url.find('bison.org'):
                        rec = None
                    else:    
                        rec['providerID'] = orgkey
                        # organization title and url
                        rec['institutionCode'] = title
                        rec['institutionID'] = url
        return rec

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
            # Previously tested for existence of these fields
            # Save a dict of sciname: taxonKeyList
            try:
                sciname = rec['scientificName']
                taxkey = rec['taxonKey']
            except Exception as e:
                self._log.info('Rec gbifID {} missing name-related field ({})'
                               .format(gid, e))
                raise 
            else:
                try:
                    keylist = self._nametaxa[sciname]
                    if taxkey not in keylist:
                        self._nametaxa[sciname].append(taxkey)
                except KeyError:
                    self._nametaxa[sciname] = [taxkey]

    # ...............................................
    def _write_namevals_for_lookup(self):
        header=['scientificName', 'taxonKeys']
        # Write scientific names and taxonKeys found with them in raw data
        fmode = 'w'        
        if os.path.exists(self.nametaxa_fname):
            fmode = 'a'
            
        try:
            writer, outf = getCSVWriter(self.nametaxa_fname, self._bison_delimiter, 
                                        self._encoding, fmode=fmode)
            self._log.info('Opened name/taxa file {} for write'.format(self.nametaxa_fname))
    
            if fmode == 'w' and header is not None:
                writer.writerow(header)
                for sciname, txkeylst in self._nametaxa.items():
                    row = [k for k in txkeylst]
                    row.insert(0, sciname)
                    writer.writerow(row)
        finally:
            outf.close()
        
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
    def _read_name_taxa(self, fname):
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
                            # First item is scientificName, rest are taxonKeys
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
    
    # ...............................................
    def _read_lookup(self, fname, uuidkey):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  dictionary of record values
        '''
        lookup = {}
        if os.path.exists(fname):
            try:
                rdr, inf = getCSVDictReader(fname, self._bison_delimiter, 
                                            self._encoding)
                for data in rdr:
                    uuid = data[uuidkey]
                    lookup[uuid] = data
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(fname, e))
            finally:
                inf.close()
        if lookup:
            self._log.info('Lookup table size for {}:'.format(fname))
            self._log.info(asizeof.asized(lookup).format())
        return lookup

    # ...............................................
    def read_dataset_lookup(self, logname=None):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  dictionary of record values
        '''
        self._rotate_logfile(logname=logname)        
        lut_fname = self.dset_lut_fname
        lut_key = GBIF_UUID_KEY
        lookup = self._read_lookup(lut_fname, lut_key)
        self._ds_lut = lookup
    
    # ...............................................
    def read_org_lookup(self, logname=None):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  dictionary of record values
        '''
        self._rotate_logfile(logname=logname)        
        lut_fname = self.org_lut_fname
        lut_key = GBIF_UUID_KEY
        lookup = self._read_lookup(lut_fname, lut_key)
        self._org_lut = lookup

    # ...............................................
    def read_name_lookup(self, logname=None):
        '''
        @summary: Read and populate dictionary with key = uuid and 
                  dictionary of record values
        '''
        self._rotate_logfile(logname=logname)        
        lookup = {}
        if os.path.exists(self.name_lut_fname):
            try:
                reader, inf = getCSVReader(self.name_lut_fname, 
                                        self._bison_delimiter, self._encoding)
                recno = 0
                line, recno = getLine(reader, recno)
                while (line is not None):
                    recno += 1
                    if (recno % LOGINTERVAL) == 0:
                        self._log.info('*** Record number {} ***'.format(recno))
                    if len(line) == 2:
                        lookup[line[0]] = line[1]
                    else:
                        self._log.error('Bad line in lookup table, recno {}, line {}'
                                        .format(recno, line))

                    # Get interpreted record
                    line, recno = getLine(reader, recno)
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(self.name_lut_fname, e))
            finally:
                inf.close()
                
            if lookup:
                self._log.info('Lookup table size for {}:'.format(self.name_lut_fname))
                self._log.info(asizeof.asized(lookup).format())
        self._name_lut = lookup


    # ...............................................
    def _open_pass1_files(self, pass1_fname):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                  output file for writing
        '''
        # Extract relevant GBIF metadata
        self._log.info('Read metadata ...')
        self.fldMeta = self.get_field_meta()

        # Open raw GBIF data
        self._log.info('Open raw GBIF input file {}'.format(self.interp_fname))
        rdr, inf = getCSVReader(self.interp_fname, self._gbif_delimiter, 
                                self._encoding)
        self._files.append(inf) 
        
        # Open output BISON file 
        self._log.info('Open step1 BISON output file {}'.format(pass1_fname))
        wtr, outf = getCSVWriter(pass1_fname, self._bison_delimiter, 
                                 self._encoding)
        self._files.append(outf)
        wtr.writerow(ORDERED_OUT_FIELDS)

        # Read any existing values for lookup
        if os.path.exists(self.nametaxa_fname):
            self._log.info('Read metadata ...')
            self._nametaxa = self._read_name_taxa(self.nametaxa_fname)
            
        return rdr, wtr
            
    # ...............................................
    def _open_pass2_files(self, pass1_fname, pass2_fname):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                  output file for writing
        '''
        infname = os.path.join(self.tmppath, pass1_fname)
        outfname = os.path.join(self.tmppath, pass2_fname)

        if not os.path.exists(infname):
            raise Exception('First pass output file {} missing!'.format(infname))
        # Open pass1 BISON file as input
        self._log.info('Open first pass input file {}'.format(infname))
        drdr, inf = getCSVDictReader(infname, self._bison_delimiter,
                                     self._encoding)
        self._files.append(inf) 

        # Open pass2 BISON file for output
        self._log.info('Open second pass output file {}'.format(outfname))
        dwtr, outf = getCSVDictWriter(outfname, self._bison_delimiter, 
                                      self._encoding, ORDERED_OUT_FIELDS)
        dwtr.writeheader()
        self._files.append(outf)
        
        
                
        return drdr, dwtr
            
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
    def _create_bisonrec_pass1(self, iline):
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
        # Fill dataset (aka resource) first
        rec = self._add_resource_values(rec)
        # Fill organization (aka provider) next, with org key from dataset values
        rec = self._add_provider_values(rec)
        
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

#     # ...............................................
#     def _update_bisonrec_pass2(self, rec):
#         clean_name = self._name_lut[rec['scientificName']]
#         rec['clean_provided_scientific_name'] = clean_name
#         return rec
    
    # ...............................................
    def transform_gbif_to_bison_pass1(self, gbif_fname, meta_fname, outfname, 
                            logname=None):
        """
        @summary: Create a CSV file containing GBIF occurrence records extracted 
                     from the interpreted occurrence file provided 
                     from an Occurrence Download, in Darwin Core format.  
                     Individual values may be calculated, modified or 
                     entire records discarded according to BISON requests.
        @return: A CSV file of first pass of BISON-modified records from a 
                 GBIF download. 
        @note: Some fields will be filled in on subsequent processing.
        """
        self.interp_fname = os.path.join(self.inpath, gbif_fname)
        self._meta_fname = os.path.join(self.inpath, meta_fname)
        pass1_fname = os.path.join(self.tmppath, outfname)

        if self.is_open():
            self.close()
        if os.path.exists(pass1_fname):
            raise Exception('First pass output file {} exists!'.format(pass1_fname))            
        self._rotate_logfile(logname=logname)        

        recno = 0
        try:
            reader, writer = self._open_pass1_files(pass1_fname)
            
            # Pull the header row 
            header, recno = getLine(reader, recno)
            line = header
            while (line is not None):
                
                # Get interpreted record
                line, recno = getLine(reader, recno)
                if line is None:
                    break
                elif (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                    
                # Create new record or empty list
                biline = self._create_bisonrec_pass1(line)
                
                # Write new record
                if biline:
                    writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self._write_namevals_for_lookup()
            
    # ...............................................
    def update_bison_pass2(self, pass1_fname, pass2_fname, logname=None):
        """
        @summary: Create a CSV file from pre-processed GBIF data, with
                  clean_provided_scientific_name resolved from 
                  original scientificName or taxonKey. 
        @return: A CSV file of BISON-modified records from a GBIF download. 
        """
        if self.is_open():
            self.close()
        self._rotate_logfile(logname=logname)

        loginterval = 1000000
        recno = 0
        try:
            dreader, dwriter = self._open_pass2_files(pass1_fname, pass2_fname)
            for rec in dreader:
                recno += 1
                clean_name = None
                gid = rec['gbifID']
                # Update record
                try:
                    oldname = rec['scientificName']
                except Exception as e:
                    self._log.warning('Failed to get sciname from rec {}, gbifID {}'
                                      .format(recno, gid))
                try:
                    clean_name = self._name_lut[oldname]
                except Exception as e:
                    self._log.warning('Failed to get clean name from sciname {} in LUT'
                                      .format(oldname))
                                      
                rec['clean_provided_scientific_name'] = clean_name
                dwriter.writerow(rec)
                if (recno % loginterval) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed reading data from line {} in {}: {}'
                            .format(recno, pass1_fname, e))                    
        finally:
            self.close()

    # ...............................................
    def _get_dataset_uuids(self):
        """
        @summary: Get dataset UUIDs from downloaded dataset EML filenames.
        """
        uuids = []
        dsfnames = glob.glob(os.path.join(self._dataset_pth, '*.xml'))
        if dsfnames is not None:
            start = len(self._dataset_pth)
            if not self._dataset_pth.endswith(os.pathsep):
                start += 1
            stop = len('.xml')
            for fn in dsfnames:
                uuids.append(fn[start:-stop])
        self._log.info('Read {} dataset UUIDs from filenames in {}'
                       .format(len(uuids), self._dataset_pth))
        return uuids
    
    # ...............................................
    def _get_organization_uuids(self):
        """
        @summary: Get organization UUIDs from dataset metadata pulled from GBIF
                  and written to the dset_lut_fname file
        """
        org_uuids = set()
        try:
            rdr, inf = getCSVDictReader(self.dset_lut_fname, OUT_DELIMITER, ENCODING)
            for dset_data in rdr:
                orgUUID = dset_data[GBIF_ORG_UUID_FOREIGN_KEY]
                org_uuids.add(orgUUID) 
        except Exception as e:
            print('Failed read {} ({})'.format(self.dset_lut_fname, e))
        finally:
            inf.close()
        self._log.info('Read {} unique organiziation UUIDs from datasets in {}'
                       .format(len(org_uuids), self.dset_lut_fname))
        return org_uuids

    # ...............................................
    def write_dataset_org_lookup(self, logname=None):
        """
        @summary: Create lookup table for: 
                  BISON resource and provider from 
                  GBIF datasetKey and dataset publishingOrganizationKey
        @return: One file, containing dataset metadata, 
                           including publishingOrganization metadata 
                           for that dataset
        """
        if os.path.exists(self.dset_lut_fname):
            raise Exception('Output file {} exists!'.format(self.dset_lut_fname))
        self._rotate_logfile(logname=logname)
        gbifapi = GbifAPI()
         
        # --------------------------------------
        # Gather dataset UUIDs from EML files downloaded with raw data
        uuids = self._get_dataset_uuids()        
        # Query/save dataset information
        gbifapi.get_write_dataset_meta(self.dset_lut_fname, uuids)
        self._log.info('Wrote dataset metadata to {}'
                       .format(self.dset_lut_fname))
        
        if not os.path.exists(self.dset_lut_fname):
            raise Exception('Dataset meta file {} does not exist'.format(self.dset_lut_fname))
        if os.path.exists(self.org_lut_fname):
            if overwrite:
                os.remove(self.org_lut_fname)
            else:
                raise Exception('Output file {} exists!'.format(self.org_lut_fname))
        # --------------------------------------
        # Gather organization UUIDs from dataset metadata assembled above
        org_uuids = self._get_organization_uuids()
        # Query/save organization information
        gbifapi.get_write_org_meta(self.org_lut_fname, org_uuids)
        self._log.info('Wrote organization metadata to {}'
                       .format(self.org_lut_fname))
        
    # ...............................................
    def _write_resolved_taxkeys(self, name_fails, nametaxa, gbifapi):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """                
        csvwriter, f = getCSVWriter(self.name_lut_fname, OUT_DELIMITER, 
                                        ENCODING, fmode='a')
        count = 0
        tax_resolved = []
        try:
            for badname in name_fails:
                taxonkeys = nametaxa[badname]
                for tk in taxonkeys:
                    canonical = gbifapi.find_canonical(taxkey=tk)
                    if canonical is not None:
                        count += 1
                        csvwriter.writerow([badname, canonical])
                        self._log.info('Appended {} taxonKey/clean_provided_scientific_name to {}'
                                       .format(count, self.name_lut_fname))
                        tax_resolved.append(badname)
                        break
        except Exception as e:
            pass
        finally:
            f.close()
            
        for tres in tax_resolved:
            name_fails.remove(tres)        
        return name_fails
            
    # ...............................................
    def _write_parsed_names(self, namelst, gbifapi):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        tot = 1000 
        name_fails = []
        while namelst:
            currnames = namelst[:tot]
            namelst = namelst[tot:]
            total, currfail = gbifapi.get_write_parsednames(currnames, 
                                                 self.name_lut_fname)
            name_fails.extend(currfail)
            self._log.info('Wrote {} sciname/cleaned pairs ({} failed) to {}'
                           .format(total-len(currfail), len(currfail), 
                                   self.name_lut_fname))        
        return name_fails
            
    # ...............................................
    def write_name_lookup(self, logname=None):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        if not os.path.exists(self.nametaxa_fname):
            raise Exception('Input file {} missing!'.format(self.nametaxa_fname))
        if os.path.exists(self.name_lut_fname):
            raise Exception('Output LUT file {} exists!'.format(self.name_lut_fname))
        self._rotate_logfile(logname=logname)
        
        # Read name/taxonIDs dictionary for name resolution
        nametaxa = self._read_name_taxa(self.nametaxa_fname)
        namelst = list(nametaxa.keys())
        gbifapi = GbifAPI()
        
        name_fails = self._write_parsed_names(namelst, gbifapi)        
        name_fails = self._write_resolved_taxkeys(name_fails, nametaxa, gbifapi)
        
        return name_fails
            
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Parse a GBIF occurrence dataset downloaded
                                     from the GBIF occurrence web service in
                                     Darwin Core format into BISON format.  
                                 """))
    parser.add_argument('gbif_occ_file', type=str, 
                        help="""
                        Absolute pathname of the input GBIF occurrence file 
                        for data transform.  Path contain downloaded GBIF data 
                        and metadata.  If the subdirectories 'tmp' and 'out' 
                        are not present in the same directory as the raw data, 
                        they will be  created for temp and final output files.
                        """)
    parser.add_argument('--step', type=int, default=1, choices=[1,2,3,4],
                        help="""
                        Step number for data processing:
                           1: Dataset/Provider lookup table assembly:
                              * Query GBIF dataset API + datasetKey for 
                                dataset info for Bison 'resource' fields and 
                                (publishing)OrganizationKey.
                              * Query GBIF organization API + organizationKey for 
                                organization info for BISON 'provider' fields'
                           2: GBIF record and field filter/transform.
                              Names and UUIDs are saved in records for GBIF API 
                              resolution in Step 2.
                           3: Name lookup table assembly:
                              * Query GBIF parser + scientificName if available, 
                                or GBIF species API + taxonKey for BISON 
                                'clean_provided_scientific_name'.
                           3: Field value replacements. Names, resource, and 
                              provider values are replaced with resolved values
                              saved in lookup tables.
                        """)
    args = parser.parse_args()
    gbif_occ_file = args.gbif_occ_file
    step = args.step

    #------------------------------------------------
    # Testing arg3uments
    #------------------------------------------------
#     gbif_occ_file = '/tank/data/bison/2019/AS/occurrence.txt'
#     step = 4
    #------------------------------------------------
    
    overwrite = True
    tmpdir = 'tmp'
    outdir = 'out'
    inpath, gbif_fname = os.path.split(gbif_occ_file)
    gbif_basefname, ext = os.path.splitext(gbif_fname)

    tmppath = os.path.join(inpath, tmpdir)
    outpath = os.path.join(inpath, outdir)
    os.makedirs(tmppath, mode=0o775, exist_ok=True)
    os.makedirs(outpath, mode=0o775, exist_ok=True)
    
    outbase = 'step{}_{}'.format(step, gbif_basefname)   
    pass1_fname = 'step2_{}.csv'.format(gbif_basefname)
    pass2_fname = 'step4_{}.csv'.format(gbif_basefname)
    
    if not os.path.exists(gbif_occ_file):
        raise Exception('Filename {} does not exist'.format(gbif_occ_file))
    else:
        gr = GBIFReader(inpath, tmpdir, outdir)
        if step == 1:
            gr.write_dataset_org_lookup(logname=outbase)
            
        elif step == 2:
            gr.read_dataset_lookup(logname=outbase)
            gr.read_org_lookup()
            gr.transform_gbif_to_bison_pass1(gbif_fname, META_FNAME, pass1_fname)
            
        elif step == 3:
            gr.write_name_lookup(logname=outbase)
            
        elif step == 4:
            gr.read_name_lookup(logname=outbase)
            gr.update_bison_pass2(pass1_fname, pass2_fname)
    
    
"""
wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv

python3.6 /state/partition1/git/bison/src/gbif/gbif2bison.py 

"""
