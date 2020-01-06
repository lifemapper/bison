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
import os
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_VALS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVReader, getCSVDictReader, getCSVWriter, 
                          getCSVDictWriter, getLine, getLogger)

from gbif.constants import (GBIF_DELIMITER, TERM_CONVERT, META_FNAME, 
                            BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS, 
                            CLIP_CHAR, FillMethod,GBIF_UUID_KEY)
from gbif.gbifmeta import GBIFMetaReader
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
    def __init__(self, basepath, tmpdir, outdir, logname):
        """
        @summary: Constructor
        @param interpreted_fname: Full filename containing records from the GBIF 
                 interpreted occurrence table
        @param meta_fname: Full filename containing metadata for all data files in the 
                 Darwin Core GBIF Occurrence download:
                     https://www.gbif.org/occurrence/search
        @param outfname: Full filename for the output BISON CSV file
        """
        # Remove any trailing /
        self.basepath = basepath.rstrip(os.sep)
        self.outpath = os.path.join(basepath, outdir)
        self.tmppath = os.path.join(basepath, tmpdir)
        self._dataset_pth = os.path.join(self.basepath, 'dataset')
        # Save these fields during processing to fill or compute from GBIF data
        self._save_flds = []
        # Ordered output fields
        self._bison_ordered_flds = []
        # Map of gbif fields to save onto bison fieldnames
        self._gbif_bison_map = {}
        # Fields to compute
        self._calc_pass1 = []
        
        for (bisonfld, fld_or_mthd) in BISON_GBIF_MAP:
            self._save_flds.append(bisonfld)
            if bisonfld not in DISCARD_FIELDS:
                self._bison_ordered_flds.append(bisonfld)
            
            if fld_or_mthd == FillMethod.pass1():
                self._calc_pass1.append(bisonfld)
            elif not FillMethod.is_calc(fld_or_mthd):
                # remove namespace designation
                gbiffld = fld_or_mthd[fld_or_mthd.rfind(CLIP_CHAR)+1:]
                self._gbif_bison_map[gbiffld] = bisonfld
        
        logfname = os.path.join(self.tmppath, '{}.log'.format(logname))
        self._log = getLogger(logname, logfname)

        self._rotate_logfile(logname=logname)
        self._files = []
        # Canonical lookup tmp data, header: scientificName, taxonKeys
        self._nametaxa = {}
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        self._gbif_line = None
        self._gbif_column_map = None
        
    # ...............................................
    def _discard_fields_from_output(self, fldlist):
        newflds = self._bison_ordered_flds.copy()
        for fld in fldlist:                
            total = newflds.count(fld)
            if total == 1:
                newflds.remove(fld)
            elif total == 0:
                self._log.error('Field {} does not exist in expected output fields'
                                .format(fld))
            elif total > 0:
                self._log.error('{} instances of field {} in expected output fields'
                                .format(total, fld))
        return newflds
            
    # ...............................................
    def _rotate_logfile(self, logname=None):
        if self._log is None:
            if logname is None:
                nm, _ = os.path.splitext(os.path.basename(__file__))
                logname = '{}.{}'.format(nm, int(time.time()))
            logfname = os.path.join(self.tmppath, '{}.log'.format(logname))
            self._log = getLogger(logname, logfname)

    # ...............................................
    def _discard_bison_add_resource_provider(self, rec, datasets, orgs):
        """
        @summary: Update the resource values from dataset key and metadata LUT.
                  Update the provider values from publishingOrganizationKey key 
                  in dataset metadata, and LUT from organization metadata.
                  Discard records with bison url for dataset or organization 
                  homepage.
        @note: function modifies or deletes original dict
        """
        orgkey = None
        if rec is not None:
            # GBIF Dataset maps to BISON resource, values from GBIF API query
            dskey = rec['resource_id']
            if dskey is not None:
                try:
                    metavals = datasets.lut[dskey]
                except:
                    self._log.warning('{} missing from dataset LUT'.format(dskey))
                else:
                    # Get organization UUID from dataset metadata
                    orgkey = metavals['publishingOrganizationKey']
                    if not orgkey:
                        self._log.warning('No organization key found for dataset {}'
                                            .format(dskey))

                    title = metavals['title']
                    url = metavals['homepage']
                    if url.find('bison.') >= 0:
                        self._log.info('Discard rec {}: dataset URL {}'
                                       .format(rec[OCC_UUID_FLD], url))
                        rec = None
                    else:    
                        rec['provider_id'] = orgkey    
                        rec['resource'] = title
                        rec['resource_url'] = url
                        
        if rec is not None and orgkey is not None:
            # GBIF Organization maps to BISON provider, retrieved from dataset 
            # above and gbif organization API query
            try:
                metavals = orgs.lut[orgkey]
            except:
                self._log.warning('{} missing from organization LUT'.format(orgkey))
            else:
#                 title = metavals['title']
                url = metavals['homepage']
                if url.find('bison.org') >= 0:
                    self._log.info('Discard rec {}: org URL {}'
                                   .format(rec[OCC_UUID_FLD], url))
                    rec = None
#                 else:    
#                     rec['provider'] = title
#                     rec['provider_url'] = url

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
        ctry = rec['iso_country_code']
        lat = lon = None
        try:
            lat = float(rec['latitude'])
        except:
            pass
        try:
            lon = float(rec['longitude']) 
        except:
            pass

        # Change 0,0 to None
        if lat == 0 and lon == 0:
            lat = lon = None
            
        # Make sure US and Canada longitude is negative
        elif (ctry in ('US', 'CA') and lon and lon > 0):
            lon = lon * -1 
            self._log.info('Rec {}: negated {} longitude to {}'
                           .format(rec[OCC_UUID_FLD], ctry, lon))
            
        # Replace values in dictionary
        rec['latitude'] = lat
        rec['longitude'] = lon

    # ...............................................
    def _update_second_choices(self, rec):
        """
        @summary: Update the verbatim_locality, with first non-blank of 
                    verbatimLocality, locality, habitat
                  Update id, with first non-blank of 
                    occurrenceId/id, recordNumber/collector_number
        @param rec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        """
        # 1st choice 
        if not rec['verbatim_locality']:
            # 2nd choice 
            locality = rec['locality'] 
            if not locality:
                # 3rd choice 
                locality = rec['habitat']
            rec['verbatim_locality'] = locality
        
        # Fill fields with secondary option if 1st is blank
        # id = 1) gbif occurrenceID or 2) gbif recordNumber (aka bison collector_number)
        # 1st choice 
        if not rec['id']:
            # 2nd choice 
            rec['id'] = rec['collector_number']

    # ...............................................
    def _update_dates(self, rec):
        """
        @summary: Make sure that eventDate is parsable into integers and update 
                     missing year value by extracting from the eventDate.
        @param rec: dictionary of all fieldnames and values for this record
        @note: BISON eventDate should be ISO 8601, ex: 2018-08-01 or 2018
                 GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        gid = rec[OCC_UUID_FLD]
        fillyr = False
        # Test year field
        try:
            rec['year'] = int(rec['year'])
        except:
            fillyr = True
            
        # Test eventDate field
        tmp = rec['occurrence_date']
        if tmp is not None:
            dateonly = tmp.split('T')[0]
            if dateonly != '':
                parts = dateonly.split('-')
                try:
                    for i in range(len(parts)):
                        int(parts[i])
                except:
                    self._log.info('Rec {}: invalid occurrence_date (gbif eventDate) {}'
                                      .format(gid, rec['occurrence_date']))
                    pass
                else:
                    rec['occurrence_date'] = dateonly
                    if fillyr:
                        rec['year'] = parts[0]

    # ...............................................
    def _control_vocabulary(self, rec):
        bor = rec['basis_of_record']
        if bor in TERM_CONVERT:
            rec['basis_of_record'] = TERM_CONVERT[bor]                

    # ...............................................
    def _read_estmeans_lookup(self, estmeans_fname):
        '''
        @summary: Read and populate dictionary with establishmentMeans 
          (concatenated list of one or more AK, HI, L48).  Keys are TSN if 
          it exists, scientificName if TSN is blank.
        @note: inputfile is tab delimited values with header:
                    scientificName    TSN    AK    HI    L48    estmeansref        
        '''
        estmeans = None
        datadict = {}
        sep = ' '
        if os.path.exists(estmeans_fname):
            try:
                drdr, inf = getCSVDictReader(estmeans_fname, '\t', ENCODING)
                for rec in drdr:
                    tsn = rec['TSN']
                    sciname = rec['scientificName']
                    emlst = []
                    for fld in ['AK', 'HI', 'L48']:
                        if rec[fld] != '':
                            emlst.append(rec[fld])
                    emstr = sep.join(emlst)                    
                    if tsn != '':
                        datadict[tsn] = emstr
                    elif sciname != '':
                        datadict[sciname] = emstr
                    else:
                        self._log.error('Record {} has no TSN or name'
                                        .format(rec.values()))
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(estmeans_fname, e))
            finally:
                inf.close()
            if datadict:
                estmeans = Lookup.initFromDict(datadict, valtype=VAL_TYPE.STRING)
        return estmeans
        
    # ...............................................
    def _read_centroid_lookup(self, terrlyr, idx_fips, idx_cnty, idx_st, 
                              idx_centroid):
        '''
        @summary: Read and populate dictionary with key = concatenated string of 
                  state name, county name, fips code and value = 
                  tuple of centroid longitude and latitude.
        '''
        centroids = None
        datadict = {}
        for poly in terrlyr:
            fips = poly.GetFieldAsString(idx_fips)
            county = poly.GetFieldAsString(idx_cnty)
            state = poly.GetFieldAsString(idx_st)
            centroid = poly.GetFieldAsString(idx_centroid)
            key = ';'.join([state, county, fips])
            tmp = centroid.lstrip('Point (').rstrip(')')
            lonstr, latstr = tmp.split(' ')
            datadict[key] = (lonstr, latstr)
        if datadict:
            centroids = Lookup.initFromDict(datadict, valtype=VAL_TYPE.TUPLE)
        return centroids
    
    # ...............................................
    def _open_pass1_files(self, gbif_interp_fname, pass1_fname, nametaxa_fname):
        '''
        @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                  output file for writing
        '''
        # Open raw GBIF data
        self._log.info('Open raw GBIF input file {}'.format(gbif_interp_fname))
        rdr, inf = getCSVReader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
        self._files.append(inf) 
        
        # Open output BISON file 
        self._log.info('Open step1 BISON output file {}'.format(pass1_fname))
        wtr, outf = getCSVWriter(pass1_fname, BISON_DELIMITER, ENCODING)
        self._files.append(outf)
        wtr.writerow(self._bison_ordered_flds)

        # Read any existing values for lookup
        if os.path.exists(nametaxa_fname):
            self._log.info('Read metadata ...')
            self._nametaxa = self._read_name_taxa(nametaxa_fname)
            
        return rdr, wtr
            
    # ...............................................
    def _open_update_files(self, inbasename, outbasename):
        '''
        @summary: Open BISON-created CSV data for reading, 
                  new output file for writing
        '''
        infname = os.path.join(self.tmppath, inbasename)
        outfname = os.path.join(self.tmppath, outbasename)

        if not os.path.exists(infname):
            raise Exception('Input file {} missing!'.format(infname))
        # Open incomplete BISON CSV file as input
        self._log.info('Open input file {}'.format(infname))
        drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
        self._files.append(inf) 
        
#         output_fields = self._discard_fields_from_output(discard_fields)
        # Open new BISON CSV file for output
        self._log.info('Open output file {}'.format(outfname))
        dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
                                      self._bison_ordered_flds)
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
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        self._gbif_line = None
        self._gbif_column_map = None

    # ...............................................
    def _test_for_discard(self, rec):
        """
        @summary: Remove record without name fields or with absence status
        @param rec: current record dictionary
        """
        if rec is not None:
            gid = rec[OCC_UUID_FLD]
            # Required fields exist 
            if (not rec['provided_scientific_name'] and not rec['taxonKey']):
                rec = None
                self._log.info('Discard rec {}: missing both sciname and taxkey'
                               .format(gid))
        if rec is not None:
            # remove records with occurrenceStatus = absence
            ostat = rec['occurrenceStatus']
            if ostat and ostat.lower() == 'absent':
                rec = None
                self._log.info('Discard rec {}: with occurrenceStatus absent'
                               .format(gid))

    # ...............................................
    def _get_gbif_val(self, gbifid, iline, gfld, idx):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        # Check column existence
        try:
            tmpval = iline[idx]
        except Exception:
            self._log.warning('Rec {}: failed to get column {}/{}'
                              .format(gbifid, idx, gfld))
            val = None
        else:
            # Test each field/value
            val = tmpval.strip()
            # Replace N/A and empty string
            if val.lower() in PROHIBITED_VALS:
                val = None
        return val

    # ...............................................
    def _gbifline_to_bisonrec(self, iline, gbif_column_map):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        gid = iline[0]
        rec = {}
        
        if len(iline) < len(self._save_flds):
            self._log.warning("""Data misalignment? 
            Only {} of {} expected fields for rec {}"""
                  .format(len(iline), len(self._save_flds), gid))
        
        for bfld in self._save_flds:
            rec[bfld] = None
        rec[OCC_UUID_FLD] = gid

        # Find values for gbif fields of interest
        for gfld, bfld in self._gbif_bison_map.items():
            # Find values for gbif fields of interest
            idx = gbif_column_map[gfld]
            val = self._get_gbif_val(gid, iline, gfld, idx)
            rec[bfld] = val
        return rec
    
#     # ...............................................
#     def _create_bisonrec_pass1(self, iline, gbif_column_map, 
#                                datasets, orgs, nametaxas):
#         """
#         @summary: Create a list of values, ordered by BISON-requested fields in 
#                      ORDERED_OUT_FIELDS, with individual values and/or entire record
#                      modified according to BISON needs.
#         @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
#         @return: list of ordered fields containing BISON-interpreted values for 
#                     a single GBIF occurrence record. 
#         """
#         biline = []
#         rec = self._gbifline_to_bisonrec(iline, gbif_column_map)
#         
#         # Check full record
#         self._test_for_discard(rec)
#         # Fill resource (gbif dataset) then provider (gbif organization) values
#         # Discard records with bison url for dataset or provider
#         self._discard_bison_add_resource_provider(rec, datasets, orgs)
#             
#         if rec is not None:
#             self._control_vocabulary(rec)
#             # Fill some fields with best non-blank option
#             self._update_second_choices(rec)
#             # Format eventDate and fill missing year
#             self._update_dates(rec)
#             # Modify lat/lon vals if necessary
#             self._update_point(rec)            
#             # Save scientificName / TaxonID for later lookup and replace
#             nametaxas.save_to_lookup(rec['provided_scientific_name'], 
#                                      rec['taxonKey'])
#             # create the ordered row
#             for fld in self._bison_ordered_flds:
#                 if not rec[fld]:
#                     biline.append('')
#                 else:
#                     biline.append(rec[fld])
#         return biline
    
    # ...............................................
    def _complete_bisonrec_pass1(self, rec, datasets, orgs, nametaxas):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        biline = []
        # Fill resource (gbif dataset) then provider (gbif organization) values
        # Discard records with bison url for dataset or provider
        self._discard_bison_add_resource_provider(rec, datasets, orgs)
            
        if rec is not None:
            # Save scientificName / TaxonID for later lookup and replace
            nametaxas.save_to_lookup(rec['provided_scientific_name'], 
                                     rec['taxonKey'])
            # create the ordered row
            for fld in self._bison_ordered_flds:
                if not rec[fld]:
                    biline.append('')
                else:
                    biline.append(rec[fld])
        return biline
    
    # ...............................................
    def _create_rough_bisonrec(self, iline, gbif_column_map):
        """
        @summary: Create a dictionary with individual values and/or entire record
                  modified according to BISON needs.
        @param line: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                 a single GBIF occurrence record. 
        """
        rec = self._gbifline_to_bisonrec(iline, gbif_column_map)        
        # Check full record
        self._test_for_discard(rec)
        if rec is not None:
            self._control_vocabulary(rec)
            # Fill some fields with best non-blank option
            self._update_second_choices(rec)
            # Format eventDate and fill missing year
            self._update_dates(rec)
            # Modify lat/lon vals if necessary
            self._update_point(rec)            
        return rec

    # ...............................................
    def transform_gbif_to_bison(self, gbif_interp_fname, dataset_lut_fname, 
                                org_lut_fname, nametaxa_fname, pass1_fname):
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
        if self.is_open():
            self.close()
        if os.path.exists(pass1_fname):
            raise Exception('First pass output file {} exists!'.format(pass1_fname))            

        datasets = Lookup.initFromFile(dataset_lut_fname, GBIF_UUID_KEY, 
                                       BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
                                       encoding=ENCODING)
        orgs = Lookup.initFromFile(org_lut_fname, GBIF_UUID_KEY, BISON_DELIMITER, 
                                   valtype=VAL_TYPE.DICT, encoding=ENCODING)
        nametaxas = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)

        recno = 0
        try:
            reader, writer = self._open_pass1_files(gbif_interp_fname, 
                                                    pass1_fname, nametaxa_fname)
            # Extract relevant GBIF metadata
            self._log.info('Read metadata ...')
            gmetardr = GBIFMetaReader(self._log)
            meta_fname = os.path.join(self.basepath, META_FNAME)
            gbif_column_map = gmetardr.get_field_meta(meta_fname)
            
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
                rec = self._create_rough_bisonrec(line, gbif_column_map)
                biline = self._complete_bisonrec_pass1(rec, datasets, orgs, nametaxas)
#                 # Create new record or empty list
#                 biline = self._create_bisonrec_pass1(line, gbif_column_map,
#                                                      datasets, orgs, 
#                                                      nametaxas)
                # Write new record
                if biline:
                    writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        nametaxas.write_lookup(nametaxa_fname, ['scientificName', 'taxonKeys'], 
                               BISON_DELIMITER)

    # ...............................................
    def find_gbif_record(self, gbifid):
        """
        @summary: Find a GBIF occurrence record identified by provided gbifID.
        """
        if (not self._gbif_reader or 
            not self._gbif_line or 
            not self._gbif_column_map):
            raise Exception('Use open_gbif_for_search before searching')

        rec = None
        try:
            while (not rec and self._gbif_line is not None):                
                # Get interpreted record
                self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 
                                                            self._gbif_recno)

                if self._gbif_line is None:
                    break
                else:
                    if self._gbif_line[0] == gbifid:
                        # Create new record or empty list
                        rec = self._create_rough_bisonrec(self._gbif_line, 
                                                          self._gbif_column_map)
                    # Where are we
                    if (self._gbif_recno % LOGINTERVAL) == 0:
                        self._log.info('*** Record number {} ***'.format(self._gbif_recno))
            if (not rec and self._gbif_line is None):
                self._log.error('Failed to find {} in remaining records'.format(gbifid))
                self.close()
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(self._gbif_recno, e))
        return rec
    
    # ...............................................
    def open_gbif_for_search(self, gbif_interp_fname):
        """
        @summary: Open a CSV file containing GBIF occurrence records extracted 
                     from the interpreted occurrence file provided 
                     from an Occurrence Download, in Darwin Core format.   
        """
        if self.is_open():
            self.close()
        # Open raw GBIF data
        self._gbif_reader, inf = getCSVReader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
        self._files.append(inf) 
        # Extract relevant GBIF metadata
        self._log.info('Read metadata ...')
        gmetardr = GBIFMetaReader(self._log)
        meta_fname = os.path.join(self.basepath, META_FNAME)
        self._gbif_column_map = gmetardr.get_field_meta(meta_fname)
        
        # Pull the header row 
        self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 0)


    # ...............................................
    def gather_name_input(self, pass1_fname, nametaxa_fname):
        recno = 0
        try:
            self._log.info('Open initial pre-processed BISON output file {}'
                           .format(pass1_fname))
            # Open output BISON file 
            dreader, inf = getCSVDictReader(pass1_fname, BISON_DELIMITER,
                                            ENCODING)
            nametaxa_lut = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)
            for rec in dreader:
                recno += 1
                nametaxa_lut.save_to_lookup(rec['provided_scientific_name'], 
                                            rec['taxonKey'])
                # Show progress
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            inf.close()

        # Write all lookup values
        nametaxa_lut.write_lookup(nametaxa_fname, 
                                  ['scientificName', 'taxonKeys'], 
                                  BISON_DELIMITER)
            
    # ...............................................
    def update_bison_names(self, infname, outfname, names, discard_fields=[]):
        """
        @summary: Create a CSV file from pre-processed GBIF data, with
                  clean_provided_scientific_name resolved from 
                  original scientificName or taxonKey. 
        @return: A CSV file of BISON-modified records from a GBIF download. 
        @return: A text file of clean_provided_scientific_names values 
        
        """
        self._bison_ordered_flds = self._discard_fields_from_output(discard_fields)
        recno = 0
        try:
            dreader, dwriter = self._open_update_files(infname, outfname)
            for rec in dreader:
                recno += 1
                clean_name = None
                gid = rec[OCC_UUID_FLD]
                # Update record
                verbatimname = rec['provided_scientific_name']
                # Update record
                try:
                    clean_name = names.lut[verbatimname]
                except Exception as e:
                    self._log.info('Rec {}: No clean name for {} in LUT'
                                   .format(gid, verbatimname))
                    taxkey = rec['taxonKey']
                    try:
                        clean_name = names.lut[taxkey]
                    except Exception as e:
                        self._log.warning('Rec {}: Discard rec with no resolution from taxkey {}'
                                          .format(gid, taxkey))
                if clean_name is not None:
                    rec['clean_provided_scientific_name'] = clean_name
#                     self._log.info('Rec {}: Replace {} with {} '
#                                    .format(gid, verbatimname, clean_name))
                    for fld in discard_fields:               
                        try:
                            rec.pop(fld)
                        except:
                            raise Exception('Field {} is not present to discard!'
                                            .format(fld))
                    dwriter.writerow(rec)
                    
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed reading data from line {} in {}: {}'
                            .format(recno, infname, e))                    
        finally:
            self.close()
            
    # ...............................................
    def _get_dataset_uuids(self, dataset_pth):
        """
        @summary: Get dataset UUIDs from downloaded dataset EML filenames.
        @param dataset_pth: absolute path to the dataset EML files
        """
        import glob
        uuids = []
        dsfnames = glob.glob(os.path.join(dataset_pth, '*.xml'))
        if dsfnames is not None:
            start = len(dataset_pth)
            if not dataset_pth.endswith(os.pathsep):
                start += 1
            stop = len('.xml')
            for fn in dsfnames:
                uuids.append(fn[start:-stop])
        self._log.info('Read {} dataset UUIDs from filenames in {}'
                       .format(len(uuids), dataset_pth))
        return uuids
    

    # ...............................................
    def write_dataset_org_lookup(self, dataset_lut_fname, org_lut_fname, delimiter=BISON_DELIMITER):
        """
        @summary: Create lookup table for: 
                  BISON resource and provider from 
                  GBIF datasetKey and dataset publishingOrganizationKey
        @return: One file, containing dataset metadata, 
                           including publishingOrganization metadata 
                           for that dataset
        """
        gbifapi = GbifAPI()
        if os.path.exists(dataset_lut_fname):
            self._log.info('Output file {} exists!'.format(dataset_lut_fname))
        else:
            gmetardr = GBIFMetaReader(self._log)
            # --------------------------------------
            # Gather dataset and organization UUIDs from EML files downloaded with 
            # raw data
            datasets = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
            dsuuids = self._get_dataset_uuids(self._dataset_pth)
            header = None
            for uuid in dsuuids:
                # query_for_dataset returns dictionary including UUID
                rec = gbifapi.query_for_dataset(uuid)
                if header is None and rec:
                    header = rec.items[0][1].keys()
                datasets.save_to_lookup(uuid, rec)
            datasets.write_lookup(dataset_lut_fname, header, delimiter)
            # Query/save dataset information
#             gbifapi.get_write_dataset_meta(dataset_lut_fname, uuids, delimiter=delimiter)
            self._log.info('Wrote dataset metadata to {}'.format(dataset_lut_fname))
            
        if os.path.exists(org_lut_fname):
            self._log.info('Output file {} exists!'.format(org_lut_fname))
        else:
            # --------------------------------------
            # Gather organization UUIDs from dataset metadata assembled above
            org_uuids = gmetardr.get_organization_uuids(dataset_lut_fname)
            # Query/save organization information
            gbifapi.get_write_org_meta(org_lut_fname, org_uuids, delimiter=delimiter)
            self._log.info('Wrote organization metadata to {}'.format(org_lut_fname))
            
            
    # ...............................................
    def _append_resolved_taxkeys(self, lut, lut_fname, name_fails, nametaxa,
                                 delimiter=BISON_DELIMITER):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        csvwriter, f = getCSVWriter(lut_fname, delimiter, ENCODING, fmode='a')
        count = 0
        names_resolved = []
        gbifapi = GbifAPI()
        try:
            for badname in name_fails:
                taxonkeys = nametaxa[badname]
                for tk in taxonkeys:
                    canonical = gbifapi.find_canonical(taxkey=tk)
                    if canonical is not None:
                        count += 1
                        lut[tk] = canonical
                        csvwriter.writerow([tk, canonical])
                        self._log.info('Appended {} taxonKey/clean_provided_scientific_name to {}'
                                       .format(count, lut_fname))
                        names_resolved.append(badname)
                        break
        except Exception as e:
            pass
        finally:
            f.close()
        for name in names_resolved:
            name_fails.remove(name)
        self._log.info('Wrote {} taxkey/canonical pairs to {} ({} unresolvable {})'
                       .format(len(names_resolved), lut_fname, 
                               len(name_fails), name_fails))                    
                        
    # ...............................................
    def _write_parsed_names(self, lut_fname, namelst, delimiter=BISON_DELIMITER):
        tot = 1000
        name_dict = {}
        name_fails = []
        csvwriter, f = getCSVWriter(lut_fname, delimiter, ENCODING, fmode='w')
        header = ['provided_scientific_name_or_taxon_key', 'clean_provided_scientific_name']
        csvwriter.writerow(header)
        gbifapi = GbifAPI()
        while namelst:
            # Write first 1000, then delete first 1000
            currnames = namelst[:tot]
            namelst = namelst[tot:]
            # Get, write parsed names
            parsed_names, currfail = gbifapi.get_parsednames(currnames)
            name_fails.extend(currfail)
            for sciname, canonical in parsed_names.items():
                name_dict[sciname] = canonical
                csvwriter.writerow([sciname, canonical])
            self._log.info('Wrote {} sciname/canonical pairs ({} failed) to {}'
                           .format(len(parsed_names), len(currfail), lut_fname))
        return name_dict, name_fails
            
            
    # ...............................................
    def resolve_write_name_lookup(self, nametaxa_fname, name_lut_fname, 
                                  delimiter=BISON_DELIMITER):
        """
        @summary: Create lookup table for: 
                  key GBIF scientificName or taxonKey, value clean_provided_scientific_name
        """
        if not os.path.exists(nametaxa_fname):
            raise Exception('Input file {} missing!'.format(nametaxa_fname))
        if os.path.exists(name_lut_fname):
            raise Exception('Output LUT file {} exists!'.format(name_lut_fname))
        
        # Read name/taxonIDs dictionary for name resolution
        nametaxa = Lookup.initFromFile(nametaxa_fname, 'scientificName', 
                                       delimiter, valtype=VAL_TYPE.SET)
        # Create name LUT with messyname/canonical from GBIF parser and save to file
        name_dict, name_fails = self._write_parsed_names(name_lut_fname, 
                                                        list(nametaxa.lut.keys()),
                                                        delimiter=delimiter)
        # Append taxonkeys/canonical from GBIF taxonkey webservice to name LUT and file
        self._append_resolved_taxkeys(name_dict, name_lut_fname, 
                                      name_fails, nametaxa)
        names = Lookup.initFromDict(name_dict, valtype=VAL_TYPE.STRING, 
                                    encoding=ENCODING)
        
        return names
            
# ...............................................
"""
import os
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_VALS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVReader, getCSVDictReader, getCSVWriter, 
                          getCSVDictWriter, getLine, getLogger)

from gbif.constants import (GBIF_DELIMITER, TERM_CONVERT, META_FNAME, 
                            BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS, 
                            CLIP_CHAR, FillMethod,GBIF_UUID_KEY)
from gbif.gbifmeta import GBIFMetaReader
from gbif.gbifapi import GbifAPI
from gbif.gbifmodify import GBIFReader

gbif_interp_file = '/tank/data/bison/2019/Terr/occurrence_lines_1-10000001.csv'
step = 10

overwrite = True
tmpdir = 'tmp'
outdir = 'out'
inpath, gbif_fname = os.path.split(gbif_interp_file)
# one level up

datapth, _ = os.path.split(inpath)
ancillary_path = os.path.join(datapth, 'ancillary')
gbif_basefname, ext = os.path.splitext(gbif_fname)

tmppath = os.path.join(inpath, tmpdir)
outpath = os.path.join(inpath, outdir)
os.makedirs(tmppath, mode=0o775, exist_ok=True)
os.makedirs(outpath, mode=0o775, exist_ok=True)

# ancillary data for record update
terrestrial_shpname = os.path.join(ancillary_path, 'US_CA_Counties_Centroids.shp')
estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
marine_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')

# reference files for lookups
dataset_lut_fname = os.path.join(tmppath, 'dataset_lut.csv')
org_lut_fname = os.path.join(tmppath, 'organization_lut.csv')
nametaxa_fname = os.path.join(tmppath, 'step1_sciname_taxkey_list.csv')
name_lut_fname = os.path.join(tmppath, 'step2_name_lut.csv')
cleanname_fname = os.path.join(tmppath, 'step2_cleanname_list.txt')
itis1_lut_fname = os.path.join(tmppath, 'step3_itis_lut.txt')

logbasename = 'step{}_{}'.format(step, gbif_basefname)
# Output CSV files of all records after initial creation or field replacements
pass1_fname = os.path.join(tmppath, 'step1_{}.csv'.format(gbif_basefname))
pass2_fname = os.path.join(tmppath, 'step2_{}.csv'.format(gbif_basefname))
pass3_fname = os.path.join(tmppath, 'step3_{}.csv'.format(gbif_basefname))

self = GBIFReader(inpath, tmpdir, outdir, logbasename)

badids = [1698383484, 1698384023, 1698384140, 1698382703, 1698382992, 
          1698383171, 1698384206, 1698384305]


self.open_gbif_for_search(gbif_interp_file)
map = self._gbif_column_map
self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, self._gbif_recno)

rec = self._create_rough_bisonrec(self._gbif_line, self._gbif_column_map)
print(rec['longitude'], rec['latitude'])
print(self._gbif_line[map['decimalLongitude']],self._gbif_line[map['decimalLatitude']]) 

id = '1912805198'
id = '1821774436'
rec = self.find_gbif_record(id)
print(rec['longitude'], rec['latitude'])
print(self._gbif_line[map['decimalLongitude']],self._gbif_line[map['decimalLatitude']]) 

for bad in badids:
    rec = self.find_gbif_record(str(bad))
    print('id {} len {}'.format(bad, len(self._gbif_line))


gr.close()

"""