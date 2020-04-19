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

from common.constants import (BISON_DELIMITER, ENCODING, 
        LOGINTERVAL, PROHIBITED_VALS, LEGACY_ID_DEFAULT, EXTRA_VALS_KEY,
        BISON_ORDERED_DATALOAD_FIELDS, BISON_IPT_PREFIX, 
        MERGED_RESOURCE_LUT_FIELDS, MERGED_PROVIDER_LUT_FIELDS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVReader, getCSVDictReader, getCSVWriter, 
                          open_csv_files)

from gbif.constants import (GBIF_DELIMITER, TERM_CONVERT, META_FNAME, 
                            BISON_GBIF_MAP, OCC_ID_FLD,
                            CLIP_CHAR, BISON_ORG_UUID, 
                            GBIF_CONVERT_TEMP_FIELDS, GBIF_NAMEKEY_TEMP_FIELD)
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
    def __init__(self, basepath, tmpdir, outdir, logger):
        """
        @summary: Constructor
        @param interpreted_fname: Full filename containing records from the GBIF 
                 interpreted occurrence table
        @param meta_fname: Full filename containing metadata for all data files in the 
                 Darwin Core GBIF Occurrence download:
                     https://www.gbif.org/occurrence/search
        @param outfname: Full filename for the output BISON CSV file
        """
        self._log = logger
        # Remove any trailing /
        self.basepath = basepath.rstrip(os.sep)
        self.outpath = os.path.join(basepath, outdir)
        self.tmppath = os.path.join(basepath, tmpdir)
        self._dataset_pth = os.path.join(self.basepath, 'dataset')
        # Save these fields during processing to fill or compute from GBIF data
        # Individual steps may add/remove temporary fields for input/output
        self._infields = BISON_ORDERED_DATALOAD_FIELDS.copy()
        # Write these fields after processing for next step
        self._outfields = BISON_ORDERED_DATALOAD_FIELDS.copy()
        # Map of gbif fields to save onto bison fieldnames
        self._gbif_bison_map = {}
        
            
        for bfld, gfld in BISON_GBIF_MAP.items():
            # remove namespace designation
            gbiffld = gfld[gfld.rfind(CLIP_CHAR)+1:]
            self._gbif_bison_map[gbiffld] = bfld
            
        self._files = []
        # Canonical lookup tmp data, header: scientificName, taxonKeys
        self._nametaxa = {}
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        
        self._missing_orgs = set()
        self._missing_datasets = set()
        
#         meta_fname = os.path.join(self.basepath, META_FNAME)
#         gmetardr = GBIFMetaReader(self._log)
#         self._gbif_column_map, self._gbif_header = \
#                         gmetardr.get_field_index_list(meta_fname)
#         self._gbif_column_map = gmetardr.get_field_meta(meta_fname)

    # ...............................................
    def _makerow(self, rec):
        row = []
        for fld in self._outfields:
            if not rec[fld]:
                row.append('')
            else:
                row.append(rec[fld])
        return row
            
    # ...............................................
    def _replace_resource_vals(self, rec, dataset_by_uuid):
        # GBIF Dataset <--> BISON resource, values from datasets LUT  
        # LUT = BISON resource table merged with GBIF dataset API
        # Header in MERGED_RESOURCE_LUT_FIELDS
        dskey = rec['datasetKey']
        dataset_title = dataset_meta_url = ''
        legacy_dataset_id = LEGACY_ID_DEFAULT
        gbif_org_uuid = legacy_org_id = None
        if dskey is not None:                    
            try:
                ds_metavals = dataset_by_uuid.lut[dskey]
            except:
                self._missing_datasets.add(dskey)
            else:
                # Get organization UUID from dataset metadata (new gbif value)
                gbif_org_uuid = ds_metavals['gbif_publishingOrganizationKey']
                if gbif_org_uuid is None or gbif_org_uuid == '':
                    gbif_org_uuid = ds_metavals['owningorganization_id']
                    if gbif_org_uuid is None or gbif_org_uuid == '':
                        self._log.warning('No gbif_org_uuid found for dataset {}'
                                          .format(dskey))
                # Get organization legacyid from old db table 
                legacy_org_id = ds_metavals['provider_id']
                if legacy_org_id is None or legacy_org_id == '':
                    legacy_org_id = ds_metavals['BISONProviderID']

                # Resource legacyid
                # from old db table value, then try new gbif value
                legacy_dataset_id = ds_metavals['OriginalResourceID']
                if legacy_dataset_id is None or legacy_dataset_id == '':
                    legacy_dataset_id = ds_metavals['gbif_legacyid']
                # Save title and url from GBIF-returned value
                dataset_title = ds_metavals['gbif_title']
                dataset_meta_url = ds_metavals['gbif_url']
                    
                if gbif_org_uuid == BISON_ORG_UUID:
                    if (dataset_meta_url is not None 
                          and dataset_meta_url.startswith(BISON_IPT_PREFIX)):
                        self._log.info('Discard rec {}: dataset URL {}'
                                       .format(rec[OCC_ID_FLD], dataset_meta_url))
                        rec = None
                else:
                    # Log other bison urls
                    if dataset_meta_url.find('bison.') >= 0:
                        self._log.info('In rec {}, found provider {} url {}'
                                       .format(rec[OCC_ID_FLD], gbif_org_uuid, 
                                               dataset_meta_url))
        if rec is not None:
            # Concat old org and dataset ids for bison resource id 
            bison_resource_id = '{},{}'.format(legacy_org_id, legacy_dataset_id)
            rec['resource_id'] = bison_resource_id
            rec['resource'] = dataset_title
            rec['resource_url'] = dataset_meta_url
                            
        return gbif_org_uuid, legacy_org_id

    # ...............................................
    def _discard_bison_add_resource_provider(self, rec, dataset_by_uuid, 
                                             org_by_uuid, org_by_legacyid):
        """
        @summary: Update the resource values from dataset key and metadata LUT.
                  Update the provider values from publishingOrganizationKey key 
                  in dataset metadata, and LUT from organization metadata.
                  Discard records with bison url for dataset or organization 
                  homepage.
        @note: function modifies or deletes original dict
        """
        if rec is not None:
            title = url = legacy_org_id = ''
            gbif_org_uuid, legacy_org_id = self._replace_resource_vals(
                rec, dataset_by_uuid)     
            # Lookup UUID
            org_metavals = None
            if gbif_org_uuid is not None:
                try:
                    org_metavals = org_by_uuid.lut[gbif_org_uuid]
                except:
                    self._missing_orgs.add(gbif_org_uuid)
                    # OR Lookup legacy id
                    if legacy_org_id is not None:
                        try:
                            org_metavals = org_by_legacyid.lut[legacy_org_id]
                        except:
                            self._missing_orgs.add(legacy_org_id)
                if org_metavals is not None:
                    # Save title and url from GBIF-returned value
                    title = org_metavals['gbif_title']
                    url = org_metavals['gbif_url']

            rec['provider_id'] = legacy_org_id
            rec['provider'] = title
            rec['provider_url'] = url

    # ...............................................
    def _update_point(self, brec):
        """
        @summary: Update the decimal longitude and latitude, replacing 0,0 with 
                     None, None and ensuring that US points have a negative longitude.
        @param brec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        @note: record must have lat/lon or countryCode but GBIF query is on countryCode so
               that will never be blank.
        """
        ctry = brec['iso_country_code']
        lat = lon = None
        try:
            lat = float(brec['latitude'])
        except:
            pass
        try:
            lon = float(brec['longitude']) 
        except:
            pass

        # Change 0,0 to None
        if lat == 0 and lon == 0:
            lat = lon = None
            
        # Make sure US and Canada longitude is negative
        elif (ctry in ('US', 'CA') and lon and lon > 0):
            lon = lon * -1 
            self._log.info('Rec {}: negated {} longitude to {}'
                           .format(brec[OCC_ID_FLD], ctry, lon))
            
        # Replace values in dictionary
        brec['latitude'] = lat
        brec['longitude'] = lon

    # ...............................................
    def _update_second_choices(self, brec):
        """
        @summary: Update the verbatim_locality, with first non-blank of 
                    verbatimLocality, locality, habitat
                  Update id, with first non-blank of 
                    occurrenceId/id, recordNumber/collector_number
        @param brec: dictionary of all fieldnames and values for this record
        @note: function modifies original dict
        """
        # 1st choice 
        if not brec['verbatim_locality']:
            # 2nd choice 
            locality = brec['locality'] 
            if not locality:
                # 3rd choice 
                locality = brec['habitat']
            brec['verbatim_locality'] = locality
        
        # Fill fields with secondary option if 1st is blank
        # id = 1) gbif occurrenceID or 2) gbif recordNumber (aka bison collector_number)
        # 1st choice 
        if not brec['id']:
            # 2nd choice 
            brec['id'] = brec['collector_number']

    # ...............................................
    def _update_dates(self, brec):
        """
        @summary: Make sure that eventDate is parsable into integers and update 
                     missing year value by extracting from the eventDate.
        @param brec: dictionary of all fieldnames and values for this record
        @note: BISON eventDate should be ISO 8601, ex: 2018-08-01 or 2018
                 GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        gid = brec[OCC_ID_FLD]
        fillyr = False
        # Test year field
        try:
            brec['year'] = int(brec['year'])
        except:
            fillyr = True
            
        # Test eventDate field
        tmp = brec['occurrence_date']
        if tmp is not None:
            dateonly = tmp.split('T')[0]
            if dateonly != '':
                parts = dateonly.split('-')
                try:
                    for i in range(len(parts)):
                        int(parts[i])
                except:
                    self._log.info('Rec {}: invalid occurrence_date (gbif eventDate) {}'
                                      .format(gid, brec['occurrence_date']))
                    pass
                else:
                    brec['occurrence_date'] = dateonly
                    if fillyr:
                        brec['year'] = parts[0]

    # ...............................................
    def _control_vocabulary(self, brec):
        bor = brec['basis_of_record']
        if bor in TERM_CONVERT:
            brec['basis_of_record'] = TERM_CONVERT[bor]                

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
    def _read_name_lookup(self, name_lut_fname):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        if not os.path.exists(name_lut_fname):
            raise Exception('Input file {} missing!'.format(name_lut_fname))
        try:
            drdr, inf = getCSVReader(name_lut_fname, BISON_DELIMITER, 
                                     ENCODING)
            for name_or_key, canonical in drdr:
                self._nametaxa[name_or_key]= canonical
        except Exception as e:
            self._log.error('Failed to interpret row {} {}, {}'
                            .format(name_or_key, canonical, e))
        finally:
            inf.close()
                
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
        wtr.writerow(self._outfields)

        # Read any existing values for lookup
        if os.path.exists(nametaxa_fname):
            self._log.info('Read metadata ...')
            self._nametaxa = self._read_name_lookup(nametaxa_fname)
            
        return rdr, wtr
            
#     # ...............................................
#     def _open_update_files(self, inbasename, outbasename):
#         '''
#         @summary: Open BISON-created CSV data for reading, 
#                   new output file for writing
#         '''
#         infname = os.path.join(self.tmppath, inbasename)
#         outfname = os.path.join(self.tmppath, outbasename)
# 
#         if not os.path.exists(infname):
#             raise Exception('Input file {} missing!'.format(infname))
#         # Open incomplete BISON CSV file as input
#         self._log.info('Open input file {}'.format(infname))
#         drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
#         self._files.append(inf) 
#         
#         # Open new BISON CSV file for output, DictWriter does not order fields
#         dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
#                                       self._outfields)
#         dwtr.writeheader()
#         self._files.append(outf)
#         return drdr, dwtr

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

    # ...............................................
    def _test_for_discard(self, brec):
        """
        @summary: Remove record without name fields or with absence status
        @param brec: current record dictionary
        """
        if brec is not None:
            gid = brec[OCC_ID_FLD]
            # Required fields exist 
            if (not brec['provided_scientific_name'] and not brec['taxonKey']):
                brec = None
                self._log.info('Discard brec {}: missing both sciname and taxkey'
                               .format(gid))
        if brec is not None:
            # remove records with occurrenceStatus = absence
            ostat = brec['occurrenceStatus']
            if ostat and ostat.lower() == 'absent':
                brec = None
                self._log.info('Discard brec {}: with occurrenceStatus absent'
                               .format(gid))

    # ...............................................
    def _get_gbif_val(self, grec, gfld):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param grec: A dictionary record of GBIF-interpreted occurrence data
        @param gfld: A GBIF fieldname for the desired value
        @return: a value for one column of a GBIF occurrence record. 
        """
        val = None
        # Check column existence
        tmpval = grec[gfld]
        if tmpval is not None:
            # Test each field/value
            val = tmpval.strip()
            # Replace N/A and empty string
            if val.lower() in PROHIBITED_VALS:
                val = None
        return val

    # ...............................................
    def _gbif_to_bison(self, grec):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        gid = grec['gbifID']
        brec = {}
        
        try:
            extravals = grec[EXTRA_VALS_KEY]
        except Exception:
            pass
        else:
            self._log.warning("""Data misalignment? Received {} extra fields for brec {}"""
                  .format(len(extravals), gid))
        
        # Initialize record
        for bfld in self._infields:
            brec[bfld] = None

        # Fill values for gbif fields of interest
        for gfld, bfld in self._gbif_bison_map.items():
            brec[bfld] = self._get_gbif_val(grec, gfld)
        return brec
    
    # ...............................................
    def _complete_bisonrec_pass1(self, brec, dataset_by_uuid, org_by_uuid, 
                                 org_by_legacyid, nametaxas):
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
        self._discard_bison_add_resource_provider(brec, dataset_by_uuid, 
                                                  org_by_uuid, org_by_legacyid)
            
        if brec is not None:
            # Save scientificName / TaxonID for later lookup and replace
            nametaxas.save_to_lookup(brec['provided_scientific_name'], 
                                     brec['taxonKey'])
            # create the ordered row
            for fld in self._outfields:
                if not brec[fld]:
                    biline.append('')
                else:
                    biline.append(brec[fld])
        return biline
    
    # ...............................................
    def _create_rough_bisonrec(self, orig_rec):
        """
        @summary: Create a dictionary with individual values and/or entire record
                  modified according to BISON needs.
        @param orig_rec: dictionary of a GBIF DarwinCore occurrence record
        @return: dictionary of BISON-interpreted values for a single GBIF 
                 occurrence record. 
        """
        brec = self._gbif_to_bison(orig_rec)        
        # Check full record
        self._test_for_discard(brec)
        if brec is not None:
            self._control_vocabulary(brec)
            # Fill some fields with best non-blank option
            self._update_second_choices(brec)
            # Format eventDate and fill missing year
            self._update_dates(brec)
            # Modify lat/lon vals if necessary
            self._update_point(brec)            
        return brec
    
#     # ...............................................
#     def _create_lookups_for_keys(self, fname, keylst, delimiter, valtype, 
#                                  encoding):
#         lookup_list = []
#         for key in keylst:
#             lookup = Lookup.initFromFile(fname, key, delimiter, 
#                                            valtype=valtype, encoding=encoding)
#             lookup_list.append(lookup)
#         return lookup_list

    # ...............................................
    def transform_gbif_to_bison(self, gbif_interp_fname, 
                                merged_dataset_lut_fname, merged_org_lut_fname, 
                                nametaxa_fname, pass1_fname):
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

        meta_fname = os.path.join(self.basepath, META_FNAME)
        gm_rdr = GBIFMetaReader(self._log)
        gbif_header = gm_rdr.get_field_list(meta_fname)

        self._infields.extend(GBIF_CONVERT_TEMP_FIELDS) 
        self._infields.append(GBIF_NAMEKEY_TEMP_FIELD)
        self._outfields.append(GBIF_NAMEKEY_TEMP_FIELD)

        dataset_by_uuid = Lookup.initFromFile(merged_dataset_lut_fname, 
            'gbif_datasetkey', BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
            encoding=ENCODING)
        org_by_uuid = Lookup.initFromFile(merged_org_lut_fname, 
            'gbif_organizationKey', BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
            encoding=ENCODING)
        org_by_legacyid = Lookup.initFromFile(merged_org_lut_fname, 
            'OriginalProviderID', BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
            encoding=ENCODING)
        nametaxas = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)

        recno = 0
        try:
            dict_reader, inf, writer, outf = open_csv_files(gbif_interp_fname, 
                                             GBIF_DELIMITER, ENCODING, 
                                             infields=gbif_header,
                                             outfname=pass1_fname, 
                                             outfields=self._outfields,
                                             outdelimiter=BISON_DELIMITER)
            # Read any existing values for lookup
            if os.path.exists(nametaxa_fname):
                self._log.info('Read name metadata ...')
                self._nametaxa = self._read_name_lookup(nametaxa_fname)
            
            for orig_rec in dict_reader:
                recno += 1
                if orig_rec is None:
                    break
                elif (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                    
                # Create new record or empty list
                brec = self._create_rough_bisonrec(orig_rec)
                biline = self._complete_bisonrec_pass1(brec, dataset_by_uuid, 
                                                       org_by_uuid, 
                                                       org_by_legacyid, 
                                                       nametaxas)
                # Write new record
                if biline:
                    writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            inf.close()
            outf.close()
            
        self._log.info('Missing organization ids: {}'.format(self._missing_orgs))    
        self._log.info('Missing dataset ids: {}'.format(self._missing_datasets))    

        if len(nametaxas.lut) > 0:
            # Write all lookup values
            nametaxas.write_lookup(nametaxa_fname, ['scientificName', 'taxonKeys'], 
                                   BISON_DELIMITER)            
        

#     # ...............................................
#     def find_gbif_record(self, gbifid):
#         """
#         @summary: Find a GBIF occurrence record identified by provided gbifID.
#         """
#         if (not self._gbif_reader or 
#             not self._gbif_line):
#             raise Exception('Use open_gbif_for_search before searching')
# 
#         rec = None
#         try:
#             while (not rec and self._gbif_line is not None):                
#                 # Get interpreted record
#                 self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 
#                                                             self._gbif_recno)
# 
#                 if self._gbif_line is None:
#                     break
#                 else:
#                     if self._gbif_line[0] == gbifid:
#                         # Create new record or empty list
#                         rec = self._create_rough_bisonrec(self._gbif_line, 
#                                                           self._gbif_column_map)
#                     # Where are we
#                     if (self._gbif_recno % LOGINTERVAL) == 0:
#                         self._log.info('*** Record number {} ***'.format(self._gbif_recno))
#             if (not rec and self._gbif_line is None):
#                 self._log.error('Failed to find {} in remaining records'.format(gbifid))
#                 self.close()
#         except Exception as e:
#             self._log.error('Failed on line {}, exception {}'.format(self._gbif_recno, e))
#         return rec
#     
#     # ...............................................
#     def open_gbif_for_search(self, gbif_interp_fname):
#         """
#         @summary: Open a CSV file containing GBIF occurrence records extracted 
#                      from the interpreted occurrence file provided 
#                      from an Occurrence Download, in Darwin Core format.   
#         """
#         if self.is_open():
#             self.close()
#         # Open raw GBIF data
#         self._gbif_reader, inf = getCSVReader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
#         self._files.append(inf) 
#         # Pull the header row 
#         self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 0)


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
    def update_bison_names(self, infname, outfname, names):
        """
        @summary: Create a CSV file from pre-processed GBIF data, with
                  clean_provided_scientific_name resolved from 
                  original scientificName or taxonKey. 
        @return: A CSV file of BISON-modified records from a GBIF download. 
        @return: A text file of clean_provided_scientific_names values 
        
        """
        self._infields.append(GBIF_NAMEKEY_TEMP_FIELD)
        recno = 0
        try:
            dict_reader, inf, writer, outf = open_csv_files(infname, 
                                             BISON_DELIMITER, ENCODING, 
                                             outfname=outfname, 
                                             outfields=self._outfields)
            for rec in dict_reader:
                recno += 1
                clean_name = None
                gid = rec[OCC_ID_FLD]
                # Update record
                verbatimname = rec['provided_scientific_name']
                # Update record
                try:
                    clean_name = names.lut[verbatimname]
                except Exception as e:
                    self._log.info('Rec {}: No clean name for {} in LUT'
                                   .format(gid, verbatimname))
                    taxkey = rec[GBIF_NAMEKEY_TEMP_FIELD]
                    try:
                        clean_name = names.lut[taxkey]
                    except Exception as e:
                        self._log.warning('Rec {}: Discard rec with no resolution from taxkey {}'
                                          .format(gid, taxkey))
                if clean_name is not None:
                    rec['clean_provided_scientific_name'] = clean_name
                    row = self._makerow(rec)
                    writer.writerow(row)
                    
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed reading data from line {} in {}: {}'
                            .format(recno, infname, e))                    
        finally:
            inf.close()
            outf.close()
            
    # ...............................................
    def _get_dataset_uuids(self):
        """
        @summary: Get dataset UUIDs from downloaded dataset EML filenames.
        @param dataset_pth: absolute path to the dataset EML files
        """
        import glob
        uuids = set()
        dsfnames = glob.glob(os.path.join(self._dataset_pth, '*.xml'))
        if dsfnames is not None:
            start = len(self._dataset_pth)
            if not self._dataset_pth.endswith(os.pathsep):
                start += 1
            stop = len('.xml')
            for fn in dsfnames:
                uuids.add(fn[start:-stop])
        self._log.info('Read {} dataset UUIDs from filenames in {}'
                       .format(len(uuids), self._dataset_pth))
        return uuids
    
    # ...............................................
    def _write_merged_org_lookup(self, org_uuids, gbifapi, old_providers, 
                                 merged_org_lut_fname, outdelimiter):
        """Write a lookup table for BISON provider (GBIF organization).
        
        The lookup merges the old BISON provider db table, with current GBIF 
        metadata values for the GBIF organization UUID.
        
        Args:
            org_uuids: list of GBIF organization UUIDs from dataset records
            gbifapi: gbif.gbifapi.GbifAPI to query GBIF web services
            old_providers: common.lookup.Lookup to manage reading/writing lookup 
                for old providers db table
            merged_org_lut_fname: output file for the merged table
            outdelimiter: field value separator for the output file
        """
        resolved_uuids = set()
        merged_lut = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        header = [fld for (fld, _) in MERGED_PROVIDER_LUT_FIELDS]
        
        # First, get new values for record in old table
        for legacyid, oldrec in old_providers.lut.items():
            uuid = oldrec['organization_id']
            resolved_uuids.add(uuid)
            # get metadata dictionary newrec from GBIF
            if uuid is not None and len(uuid) > 20:
                newrec = gbifapi.query_for_organization(uuid)
            else:
                newrec = gbifapi.query_for_organization(legacyid, 
                                                        is_legacyid=True)
            # Add newrec keys and their values to oldrec
            for key, val in newrec.items():
                oldrec[key] = val
            # Save all keys/vals to merged LUT
            merged_lut.save_to_lookup(legacyid, oldrec)

        # Next, get metadata for any UUIDs not already resolved
        unresolved_uuids = org_uuids.difference(resolved_uuids)
        for uuid in unresolved_uuids:
            newrec = gbifapi.query_for_organization(uuid)
            legacyid = newrec['gbif_legacyid']
            # Save new keys/vals to merged LUT
            if newrec['gbif_legacyid'] != LEGACY_ID_DEFAULT:
                merged_lut.save_to_lookup(legacyid, oldrec)
            else:
                merged_lut.save_to_lookup(uuid, oldrec)

        merged_lut.write_lookup(merged_org_lut_fname, header, outdelimiter)
        self._log.info('Wrote organization metadata to {}'.format(
            merged_org_lut_fname))

    # ...............................................
    def _write_org_lookup_OLD(self, org_uuids, gbifapi, old_providers, org_lut_fname, 
                          outdelimiter):
        """
        @summary: Create lookup table for: 
                  BISON provider (GBIF organization) from GBIF organizationKey 
                  and BISON-provided provider table (containing legacy ids)
        @postcondition: A file has been written containing dataset metadata 
                        for each dataset
        """
        providers = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        header = None
        for uuid in org_uuids:
            try:
                oldvals = old_providers.lut[uuid]
            except:
                self._log.warning('{} missing from BISON provider table for legacyid'
                                  .format(uuid))
                old_legacy_id = LEGACY_ID_DEFAULT
            else:
                old_legacy_id = oldvals['legacyid']
            # query_for_organization returns dictionary including UUID
            rec = gbifapi.query_for_organization(uuid)
            # old legacy id takes precedence
            new_legacy_id = rec['legacyid']
            if new_legacy_id == LEGACY_ID_DEFAULT:
                legacy_id = old_legacy_id 
            elif old_legacy_id == LEGACY_ID_DEFAULT:
                legacy_id = new_legacy_id
            else:
                legacy_id = old_legacy_id
                if new_legacy_id != old_legacy_id:
                    print('Provider old_legacy_id {} != gbif organization legacy id {}'
                          .format(old_legacy_id, new_legacy_id))
            rec['legacyid'] = legacy_id

            if header is None and rec:
                header = list(rec.keys())
            providers.save_to_lookup(uuid, rec)
        providers.write_lookup(org_lut_fname, header, outdelimiter)
        self._log.info('Wrote organization metadata to {}'.format(org_lut_fname))

    # ...............................................
    def _write_dataset_lookup_OLD(self, gbifapi, resources, dataset_lut_fname, 
                              outdelimiter):
        """
        @summary: Create lookup table for: 
                  BISON resource (GBIF dataset) from GBIF datasetKey and 
                  BISON-provided resource table (containing legacy ids)
        @return: Dataset Lookup
        @postcondition: A file has been written containing dataset metadata 
                        for each dataset
        """
        # Gather dataset and organization UUIDs from EML files downloaded with 
        # raw data
        datasets = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        dsuuids = self._get_dataset_uuids()
        header = None
        for uuid in dsuuids:
            try:
                oldvals = resources.lut[uuid]
            except:
                self._log.warning('{} missing from BISON resources table for legacyid'
                                  .format(uuid))
                old_legacy_id = LEGACY_ID_DEFAULT
            else:
                old_legacy_id = oldvals['legacyid']
            # query_for_dataset returns dictionary including UUID
            rec = gbifapi.query_for_dataset(uuid)
            # old legacy id takes precedence
            new_legacy_id = rec['legacyid']
            if new_legacy_id == LEGACY_ID_DEFAULT:
                legacy_id = old_legacy_id 
            elif old_legacy_id == LEGACY_ID_DEFAULT:
                legacy_id = new_legacy_id
            else:
                legacy_id = old_legacy_id
                if new_legacy_id != old_legacy_id:
                    print('Resource old_legacy_id {} != gbif dataset legacy id {}'
                          .format(old_legacy_id, new_legacy_id))
            rec['legacyid'] = legacy_id
            
            if header is None and rec:
                header = list(rec.keys())
            datasets.save_to_lookup(uuid, rec)
        datasets.write_lookup(dataset_lut_fname, header, outdelimiter)
        # Query/save dataset information
        self._log.info('Wrote dataset metadata to {}'.format(dataset_lut_fname))
        return datasets
            
    # ...............................................
    def _write_merged_dataset_lookup(self, gbifapi, old_resources, 
                                     merged_dataset_lut_fname, outdelimiter):
        """Write a lookup table for BISON resources (GBIF dataset).
        
        The lookup merges the old BISON resource db table, with current GBIF 
        metadata values for the GBIF dataset UUID.
        
        Args:
            gbifapi: gbif.gbifapi.GbifAPI to query GBIF web services
            old_resources: common.lookup.Lookup to manage reading/writing lookup 
                for old resources db table
            merged_dataset_lut_fname: output file for the merged table
            outdelimiter: field value separator for the output file
        """
        ds_uuids = self._get_dataset_uuids()
        resolved_uuids = set()
        merged_lut = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        header = [fld for (fld, _) in MERGED_RESOURCE_LUT_FIELDS]
        
        # First, get new values for record in old table
        for legacyid, oldrec in old_resources.lut.items():
            # get metadata dictionary newrec from GBIF
            uuid = oldrec['dataset_id']
            resolved_uuids.add(uuid)
            if uuid is not None and len(uuid) > 20:
                newrec = gbifapi.query_for_dataset(uuid)
            else:
                newrec = gbifapi.query_for_dataset(legacyid, is_legacyid=True)
            # Add newrec keys and their values to oldrec
            for key, val in newrec.items():
                oldrec[key] = val
            # Save all keys/vals to merged LUT
            merged_lut.save_to_lookup(legacyid, oldrec)
            
        # Next, get metadata for any UUIDs not already resolved
        unresolved_uuids = ds_uuids.difference(resolved_uuids)
        for uuid in unresolved_uuids:
            newrec = gbifapi.query_for_dataset(uuid)
            legacyid = newrec['gbif_legacyid']
            # Save new keys/vals to merged LUT
            if newrec['gbif_legacyid'] != LEGACY_ID_DEFAULT:
                merged_lut.save_to_lookup(legacyid, oldrec)
            else:
                merged_lut.save_to_lookup(uuid, oldrec)

        merged_lut.write_lookup(merged_dataset_lut_fname, header, outdelimiter)
        # Query/save dataset information
        self._log.info('Wrote dataset metadata to {}'.format(
            merged_dataset_lut_fname))
        return merged_lut
            
    # ...............................................
    def write_dataset_org_lookup(self, merged_dataset_lut_fname, resource_lut_fname, 
                                 merged_org_lut_fname, provider_lut_fname, 
                                 outdelimiter=BISON_DELIMITER):
        """Write merged BISON resource and provider lookup tables to files.
           
        Each lookup table will contain legacy table information from the 
        existing BISON database, and updated information for the GBIF UUID 
        referenced.
        
        Args:
            merged_dataset_lut_fname: output file for the merged dataset table
            resource_lut_fname: input file containing existing BISON resource
                db table.
            merged_org_lut_fname: output file for the merged organization table
            provider_lut_fname: input file containing existing BISON provider
                db table.
            outdelimiter: field value separator for the output files
           
        Results:
            Two files, one containing merged dataset metadata, and one 
            containing merged provider metadata.
        """
        gbifapi = GbifAPI()
        if os.path.exists(merged_dataset_lut_fname):
            self._log.info('Merged output file {} exists!'.format(
                merged_dataset_lut_fname))
        else:
            old_resources = Lookup.initFromFile(resource_lut_fname, 
                                                'OriginalResourceID',
#                                                 'dataset_id', 
                                                BISON_DELIMITER, 
                                                valtype=VAL_TYPE.DICT, 
                                                encoding=ENCODING)
            merged_datasets = self._write_merged_dataset_lookup(
                gbifapi, old_resources, merged_dataset_lut_fname, outdelimiter)
            
        if os.path.exists(merged_org_lut_fname):
            self._log.info('Output file {} exists!'.format(merged_org_lut_fname))
        else:
            old_providers = Lookup.initFromFile(provider_lut_fname, 
                                                'OriginalProviderID',
#                                                 'organization_id', 
                                                BISON_DELIMITER, 
                                                valtype=VAL_TYPE.DICT, 
                                                encoding=ENCODING)
    
            # --------------------------------------
            # Gather organization UUIDs from dataset metadata assembled (LUT or file)
            org_uuids = set()
            try:
                for key, ddict in merged_datasets.lut.items():
                    try:
                        org_uuids.add(ddict['gbif_publishingOrganizationKey'])
                    except Exception:
                        print('No publishingOrganizationKey in dataset {}'.format(key))
            except Exception:             
                gmetardr = GBIFMetaReader(self._log)
                org_uuids = gmetardr.get_organization_uuids(
                    merged_dataset_lut_fname)
                
            self._write_merged_org_lookup(
                org_uuids, gbifapi, old_providers, merged_org_lut_fname, 
                outdelimiter)
            
            
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
        except Exception:
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
        try:
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
                # If > 10% fail, test for BOLD or pause
                fail_rate = len(currfail) / tot
                if fail_rate > 0.1:
                    non_sn_count = 0
                    for sn in currfail:
                        if sn.find(':') >= 0:
                            non_sn_count += 1
                    non_sn_rate = non_sn_count / len(currfail)
                    if non_sn_rate < 0.8:
                        time.sleep(10)
                        parsed_names, currfail = gbifapi.get_parsednames(currnames)
                name_fails.extend(currfail)
                for sciname, canonical in parsed_names.items():
                    name_dict[sciname] = canonical
                    csvwriter.writerow([sciname, canonical])
                self._log.info('Wrote {} sciname/canonical pairs ({} failed) to {}'
                               .format(len(parsed_names), len(currfail), lut_fname))
        except Exception as e:
            self._log.error('Failed writing parsed names {}'.format(e))
        finally:
            f.close()
        return name_dict, name_fails
            
            
    # ...............................................
    def get_canonical_lookup(self, nametaxa_fname, name_lut_fname, 
                                  delimiter=BISON_DELIMITER):
        """
        @summary: Create lookup table for: 
                  key GBIF scientificName or taxonKey, value clean_provided_scientific_name
        """
        if not os.path.exists(nametaxa_fname):
            raise Exception('Input file {} missing!'.format(nametaxa_fname))
        
        if os.path.exists(name_lut_fname):
            # Read existing lookup
            self._log.info('Output LUT file {} exists'.format(name_lut_fname))
            self._read_name_lookup(name_lut_fname)
            canonical_lut = Lookup.initFromDict(self._nametaxa, 
                                                valtype=VAL_TYPE.STRING, 
                                                encoding=ENCODING)
        else:
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
            canonical_lut = Lookup.initFromDict(name_dict, 
                                                valtype=VAL_TYPE.STRING, 
                                                encoding=ENCODING)
        
        return canonical_lut


# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Find a GBIF occurrence record from a dataset downloaded
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
    parser.add_argument('gbifid', type=str, 
                        help="""
                        GBIF identifier for the record to find.
                        """)
    args = parser.parse_args()
    gbif_interp_file = args.gbif_occ_file
    gbifid = args.gbifid

    overwrite = True
    tmpdir = 'tmp'
    outdir = 'out'
    inpath, gbif_fname = os.path.split(gbif_interp_file)

    gr = GBIFReader(inpath, tmpdir, outdir, 'test')

    gr.open_gbif_for_search(gbif_interp_file)
    badids = []

#     id = '1325646994'
    rec = gr.find_gbif_record(gbifid)
    
#     print(rec['longitude'], rec['latitude'])
#     print(gr._gbif_line[map['decimalLongitude']],gr._gbif_line[map['decimalLatitude']])
    
    
    badids.append(id)
    for bad in badids:
        rec = gr.find_gbif_record(str(bad))
        print('id {} len {}'.format(bad, len(gr._gbif_line)))

    gr.close()
"""
import os
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_VALS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVReader, getCSVDictReader, getCSVWriter, 
                          getCSVDictWriter, getLine, getLogger)

from gbif.constants import (GBIF_DELIMITER, TERM_CONVERT, META_FNAME, 
                            BISON_GBIF_MAP, OCC_ID_FLD, DISCARD_FIELDS, 
                            CLIP_CHAR, GBIF_UUID_KEY)
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
canonical_lut_fname = os.path.join(tmppath, 'canonical_lut.csv')

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
id = '1325646994'
for bad in badids:
    rec = self.find_gbif_record(str(bad))
    print('id {} len {}'.format(bad, len(self._gbif_line))


gr.close()

gmetardr = GBIFMetaReader(self._log)
datasets = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
dsuuids = self._get_dataset_uuids(self._dataset_pth)
header = None
for uuid in dsuuids:
    rec = gbifapi.query_for_dataset(uuid)
    if header is None and rec:
        header = rec.items[0][1].keys()
    datasets.save_to_lookup(uuid, rec)


"""