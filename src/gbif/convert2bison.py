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
from osgeo import ogr
import os
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL)
from common.itissvc import ITISSvc
from common.lookup import Lookup
from common.tools import (getCSVReader, getCSVDictReader, 
                        getCSVWriter, getCSVDictWriter, getLine, getLogger)

from gbif.constants import (GBIF_DELIMITER, PROHIBITED_VALS, 
                            TERM_CONVERT, META_FNAME,
                            BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS,
                            CLIP_CHAR, FillMethod,GBIF_UUID_KEY)
from gbif.metareader import GBIFMetaReader
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
        
        self.lut = Lookup()
        
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
        
        self._log = None
        self._rotate_logfile(logname=logname)
        self._files = []
                
        # ....................
        # Canonical lookup tmp data, header: scientificName, taxonKeys
        self._nametaxa = {}
        
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
    def _discard_bison_add_resource_provider(self, rec, dataset_lut, org_lut):
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
                    metavals = dataset_lut[dskey]
                except:
                    self._log.warning('{} missing from dataset LUT'.format(dskey))
                else:
                    # Get organization UUID from dataset metadata
                    orgkey = metavals['publishingOrganizationKey']
                    if orgkey is None:
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
                metavals = org_lut[orgkey]
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
        elif (ctry in ('US', 'CA') and lon is not None  and lon > 0):
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
#         gid = rec[OCC_UUID_FLD]
        # 1st choice 
        if rec['verbatim_locality'] is None:
            # 2nd choice 
            locality = rec['locality'] 
            if locality is not None:
                # 3rd choice 
                locality = rec['habitat']
            rec['verbatim_locality'] = locality
        
        # Fill fields with secondary option if 1st is blank
        # id = 1) gbif occurrenceID or 2) gbif recordNumber (aka bison collector_number)
        # 1st choice 
        if rec['id'] is None:
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
                    self._log.info('Rec {}: invalid eventDate {}'
                                      .format(gid, rec['eventDate']))
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
        lookup = {}
        delimiter = '\t'
        sep = ' '
        if os.path.exists(estmeans_fname):
            try:
                drdr, inf = getCSVDictReader(estmeans_fname, delimiter, 
                                               ENCODING)
                eml = []
                for rec in drdr:
                    for f in ['AK', 'HI', 'L48']:
                        if rec[f] != '':
                            eml.append(rec[f])
                    estmeans = sep.join(eml)                    
                    if rec['TSN'] is not None:
                        lookup[rec['TSN']] = estmeans
                    else:
                        lookup[rec['scientificName']] = estmeans

            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(estmeans_fname, e))
            finally:
                inf.close()                
        return lookup
        
    # ...............................................
    def _read_centroid_lookup(self, centroid_lut_fname, delimiter=','):
        '''
        @summary: Read and populate dictionary with state+county+fips = coordinates
        @note: lookup table should contain comma-delimited lines with 
               state_name, county_name, fips_code, longitude, latitude
        '''
        lookup = {}
        if os.path.exists(centroid_lut_fname):
            try:
                reader, inf = getCSVReader(centroid_lut_fname, delimiter, 
                                           ENCODING)
                recno = 0
                line, recno = getLine(reader, recno)
                while (line is not None):
                    recno += 1
                    if (recno % LOGINTERVAL) == 0:
                        self._log.info('*** Record number {} ***'.format(recno))
                    try:
                        state, county, fips, lon, lat = line
                    except:
                        self._log.error('Bad line in lookup table, recno {}, line {}'
                                        .format(recno, line))
                    else:
                        key = ';'.join((state, county, fips))
                        lookup[key] = (lon, lat)

                    # Get interpreted record
                    line, recno = getLine(reader, recno)
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(name_lut_fname, e))
            finally:
                inf.close()                
        return lookup
    
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
    def _open_update_files(self, inbasename, outbasename, discard_fields=[]):
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
        
        output_fields = self._discard_fields_from_output(discard_fields)
        # Open new BISON CSV file for output
        self._log.info('Open output file {}'.format(outfname))
        dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
                                      output_fields)
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
    def _test_for_discard(self, rec):
        """
        @summary: Remove record without name fields or with absence status
        @param rec: current record dictionary
        """
        if rec is not None:
            gid = rec[OCC_UUID_FLD]
            # Required fields exist 
            if (rec['provided_scientific_name'] is None 
                    and rec['taxonKey'] is None):
                rec = None
                self._log.info('Discard rec {}: missing both sciname and taxkey'
                               .format(gid))
        if rec is not None:
            # remove records with occurrenceStatus = absence
            ostat = rec['occurrenceStatus']
            if ostat is not None and ostat.lower() == 'absent':
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

    # ...............................................
    def _create_bisonrec_pass1(self, iline, gbif_column_map, dataset_lut, org_lut):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in 
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
        @return: list of ordered fields containing BISON-interpreted values for 
                    a single GBIF occurrence record. 
        """
        biline = []
        rec = self._gbifline_to_bisonrec(iline, gbif_column_map)
        
        # Check full record
        rec = self._test_for_discard(rec)
        # Fill resource (gbif dataset) then provider (gbif organization) values
        # Discard records with bison url for dataset or provider
        rec = self._discard_bison_add_resource_provider(rec, dataset_lut, org_lut)
            
        if rec is not None:
            rec = self._control_vocabulary(rec)
            # Fill some fields with best non-blank option
            rec = self._update_second_choices(rec)
            # Format eventDate and fill missing year
            rec = self._update_dates(rec)
            # Modify lat/lon vals if necessary
            rec = self._update_point(rec)            
            # Save scientificName / TaxonID for later lookup and replace
            self.lut.save_to_lookup(rec['provided_scientific_name'], 
                                        rec['taxonKey'], self._nametaxa, 
                                        is_list=True)
            # create the ordered row
            for fld in self._bison_ordered_flds:
                if rec[fld] is None:
                    biline.append('')
                else:
                    biline.append(rec[fld])
        return biline
    
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

        dataset_lut = self.lut.read_lookup(dataset_lut_fname, GBIF_UUID_KEY)
        org_lut = self.lut.read_lookup(org_lut_fname, GBIF_UUID_KEY)

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
                biline = self._create_bisonrec_pass1(line, gbif_column_map,
                                                     dataset_lut, org_lut)
                # Write new record
                if biline:
                    writer.writerow(biline)
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self.lut.write_to_lookup(nametaxa_fname, 
                                    ['scientificName', 'taxonKeys'], 
                                    BISON_DELIMITER, isList=True)

    # ...............................................
    def gather_name_input(self, pass1_fname, nametaxa_fname):
        recno = 0
        try:
            self._log.info('Open initial pre-processed BISON output file {}'.format(pass1_fname))
            # Open output BISON file 
            dreader, inf = getCSVDictReader(pass1_fname, BISON_DELIMITER,
                                            ENCODING)
            self._files.append(inf) 
            for rec in dreader:
                recno += 1
                # Save scientificName / TaxonID for later lookup and replace
#                 self._save_namevals_for_lookup(rec)
                self.lut.save_to_lookup(rec['provided_scientific_name'], 
                                            rec['taxonKey'], self._nametaxa, 
                                            is_list=True)
                # Show progress
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                
        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            self.close()

        # Write all lookup values
        self.lut.write_lookup(nametaxa_fname, self._nametaxa, 
                                 ['scientificName', 'taxonKeys'], 
                                 BISON_DELIMITER, is_list=True)
            
    # ...............................................
    def update_bison_names(self, infname, outfname, name_lut, discard_fields=[]):
        """
        @summary: Create a CSV file from pre-processed GBIF data, with
                  clean_provided_scientific_name resolved from 
                  original scientificName or taxonKey. 
        @return: A CSV file of BISON-modified records from a GBIF download. 
        @return: A text file of clean_provided_scientific_names values 
        
        """
        if self.is_open():
            self.close()
        recno = 0
        try:
            dreader, dwriter = self._open_update_files(infname, outfname, 
                                                discard_fields=discard_fields)
            for rec in dreader:
                recno += 1
                clean_name = None
                gid = rec[OCC_UUID_FLD]
                # Update record
                verbatimname = rec['provided_scientific_name']
                taxkey = rec['taxonKey']
                # Update record
                try:
                    clean_name = name_lut[verbatimname]
                except Exception as e:
                    self._log.info('Rec {}: No clean name for {} in LUT'
                                   .format(gid, verbatimname))
                    try:
                        clean_name = name_lut[taxkey]
                    except Exception as e:
                        self._log.warning('Rec {}: Discard rec with no resolution from taxkey {}'
                                          .format(gid, taxkey))
                if clean_name is not None:
                    self._log.info('Rec {}: Replace {}/{} with {} '
                                   .format(gid, verbatimname, taxkey, clean_name))
                    rec['clean_provided_scientific_name'] = clean_name
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
    def _fill_geofields(self, rec, lon, lat,
                        terrlyr, idx_fips, idx_cnty, idx_st,
                        eezlyr, idx_eez, idx_mg):
        terrcount = 0
        marinecount = 0
        
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(lon, lat)
        terrlyr.SetSpatialFilter(pt)
        eezlyr.SetSpatialFilter(pt)
        print('Feature {}, {}:'.format(lon, lat))
        for poly in terrlyr:
            terrcount += 1
            fips = poly.GetFieldAsString(idx_fips)
            county = poly.GetFieldAsString(idx_cnty)
            state = poly.GetFieldAsString(idx_st)
        for poly in eezlyr:
            marinecount += 1
            eez = poly.GetFieldAsString(idx_eez)
            mrgid = poly.GetFieldAsString(idx_mg)
        # Single terrestrial polygon takes precedence
        if terrcount == 1:
            # terrestrial intersect
            rec['calculated_fips'] = fips
            rec['calculated_county_name'] = county
            rec['calculated_state_name'] = state
            self._log.info('  fips = {}, county={}, st={}'
                           .format(fips, county, state))
        elif terrcount == 0:
            # Single marine intersect is 2nd choice
            if marinecount == 1:
                rec['calculated_waterbody'] = eez
                rec['mrgid'] = mrgid
                self._log.info('  calculated_waterbody = {}, mrgid={}'
                               .format(eez, mrgid))
            # 0 or > 1 marine intersects
            else:
                self._log.info('  Coordinate intersects no terrestrial, {} EEZ polygons'
                               .format(marinecount))
        # terrcount > 1, multiple intersects
        else:
            self._log.info('  Coordinate intersects {} terrestrial, {} EEZ polygons'
                           .format(terrcount, marinecount))

    # ...............................................
    def _fill_centroids(self, rec, centroid_lut):
        pfips = rec['provided_fips']
        pcounty = rec['provided_county_name']
        pstate = rec['provided_state_name']
        if ((pcounty != '' and pstate != '') or pfips != ''):
            print('Provided county {}, state {}, fips {}'
                  .format(pcounty, pstate, pfips))
            key = ';'.join((pstate, pcounty, pfips))
            try:
                lon, lat = centroid_lut[key]
            except:
                pass
            else:
                rec['longitude'] = lon
                rec['latitude'] = lat
                rec['centroid'] = 'county'

    # ...............................................
    def _get_itisfields(self, name, itis_svc):
        # Get tsn, acceptedTSN, accepted_name, kingdom
        accepted_tsn = row = None
        tsn, accepted_name, kingdom, accepted_tsn_list = itis_svc.get_itis_tsn(name)
        if accepted_name is None:
            for accepted_tsn in accepted_tsn_list:
                accepted_name, kingdom = itis_svc.get_itis_name(accepted_tsn)
                if accepted_name is not None:
                    break
        else:
            accepted_tsn = tsn
        
        for v in (tsn, accepted_name, kingdom, accepted_tsn):
            if v is not None:
                row = [tsn, accepted_name, kingdom, accepted_tsn]
                break
        if row:
            # Get common names
            common_names = itis_svc.get_itis_vernacular(accepted_tsn)
            common_names_str = ';'.join(common_names)
            row.append(common_names_str)
        return row

#     # ...............................................
#     def write_itis_lookup(self, cleanname_fname, itis_lut_fname):
#         if self.is_open():
#             self.close()
#         header = ['clean_provided_scientific_name', 'itis_tsn', 
#                   'valid_accepted_scientific_name', 'valid_accepted_tsn', 
#                   'kingdom', 'itis_common_name']
#         itis_svc = ITISSvc()
#         recno = 0
#         try:
#             inf = open(cleanname_fname, 'r', encoding=ENCODING)
#             csvwriter, outf = getCSVWriter(itis_lut_fname, BISON_DELIMITER, 
#                                            ENCODING, 'w')
#             self._files.extend([inf, outf])
#             csvwriter.writerow(header)
#             for line in inf:
#                 recno += 1
#                 cleanname = line.strip()
#                 row = self._get_itisfields(cleanname, itis_svc)
#                 if row is not None:
#                     row.insert(0, cleanname)
#                     csvwriter.writerow(row)
#         except Exception as e:
#             self._log.error('Failed reading from line {} {} or writing to {}; {}'
#                             .format(recno, cleanname_fname, itis_lut_fname, e))                    
#         finally:
#             self.close()            
            
    # ...............................................
    def _fill_itisfields1(self, rec, itis_lut):
        """
        Aimee's LUT header
        clean_provided_scientific_name, itis_tsn, valid_accepted_scientific_name, 
        valid_accepted_tsn, kingdom, itis_common_name
        """
        canonical = rec['clean_provided_scientific_name']
        try:
            itis_vals = itis_lut[canonical]
        except Exception as e:
            self._log.info('Found NO itis values for {}'.format(canonical))                    
        else:
            for fld in ['itis_tsn', 'valid_accepted_scientific_name', 
                        'valid_accepted_tsn', 'kingdom', 'itis_common_name']:
                rec[fld] = itis_vals[fld]

    # ...............................................
    def _fill_itisfields2(self, rec, itis_lut):
        """
        Derek's LUT header
        scientific_name, tsn, valid_accepted_scientific_name, valid_accepted_tsn,
        hierarchy_string, common_name, amb
        """
        canonical = rec['clean_provided_scientific_name']
        try:
            itis_vals = itis_lut[canonical]
        except Exception as e:
            self._log.info('Found NO itis values for {}'.format(canonical))                    
        else:
            rec['itis_tsn'] = itis_vals['tsn']
            rec['valid_accepted_scientific_name'] = itis_vals['valid_accepted_scientific_name']
            rec['valid_accepted_tsn'] = itis_vals['valid_accepted_tsn']
            rec['itis_common_name'] = itis_vals['common_name']
            rec['kingdom'] = itis_vals['kingdom']

    # ...............................................
    def _fill_estmeans_field(self, rec, estmeans_lut):
        estmeans = None
        tsn = rec['itis_tsn']
        sname = rec['clean_provided_scientific_name']
        try:
            estmeans = estmeans_lut[tsn]
        except:
            try:
                estmeans = estmeans[sname]
            except:
                self._log.info('Found NO establishment means for {} or {}'
                               .format(tsn, sname))
        rec['establishment_means'] = estmeans

    # ...............................................
    def _get_coords(self, rec):
        lon = lat = None
        slon = rec['longitude']
        slat = rec['latitude']
        try:
            lon = float(slon)
            lat = float(slat)
        except:
            lon = lat = None
        return lon, lat

    # ...............................................
    def update_itis_geo_estmeans(self, infname, itis2_lut_fname,
                                 terr_geo_shpname, centroid_lut_fname, 
                                 eez_shpname, estmeans_fname, outfname):
        """
        @summary: Create a CSV file from pre-processed BISON data and 
                  external ITIS and Georeferencing data.
        @return: A CSV file of BISON-modified records  
        """
        if self.is_open():
            self.close()
        driver = ogr.GetDriverByName("ESRI Shapefile")
        terr_data_src = driver.Open(terr_geo_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        terr_def = terrlyr.GetLayerDefn()
        idx_fips = terr_def.GetFieldIndex('FIPS')
        idx_cnty = terr_def.GetFieldIndex('COUNTY_NAM')
        idx_st = terr_def.GetFieldIndex('STATE_NAME')
        
        eez_data_src = driver.Open(eez_shpname, 0)
        eezlyr = eez_data_src.GetLayer()
        eez_def = eezlyr.GetLayerDefn()
        idx_eez = eez_def.GetFieldIndex('EEZ')
        idx_mg = eez_def.GetFieldIndex('MRGID')

        # Derek's LUT header
        # scientific_name, tsn, valid_accepted_scientific_name, valid_accepted_tsn,
        # hierarchy_string, common_name, amb, kingdom
        itis_lut = Lookup()
        itis_lut.read_lookup(itis2_lut_fname, 'scientific_name', ',', ENCODING)
#         itis_lut = self._read_lookup(itis2_lut_fname, 'scientific_name', delimiter=',')
        centroid_lut = self._read_centroid_lookup(centroid_lut_fname, delimiter=',')
        estmeans_lut = self._read_estmeans_lookup(estmeans_fname)

        recno = 0
        try:
            dreader, dwriter = self._open_update_files(infname, outfname)
            for rec in dreader:
                recno += 1
                gid = rec[OCC_UUID_FLD]
                lon, lat = self._get_coords(rec)
                # Fill coordinates if possible
                if lon is None:
                    rec = self._fill_centroids(rec, centroid_lut)
                    lon, lat = self._get_coords(rec)
                # Use coordinates to calc 
                if lon is not None:
                    # Compute geo: coordinates and polygons
                    rec = self._fill_geofields(rec, lon, lat,
                                               terrlyr, idx_fips, idx_cnty, idx_st,
                                               eezlyr, idx_eez, idx_mg)
                # Fill ITIS 
                rec = self._fill_itisfields(rec, itis_lut)
                # Fill establishment_means from TSN or 
                # clean_provided_scientific_name and establishment means table
                rec = self._fill_estmeans_field(rec, estmeans_lut)
                # Write updated record
                dwriter.writerow(rec)
                # Log progress occasionally
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from line {}: {}'
                            .format(recno, e))                    
        finally:
            self.close()

    # ...............................................
    def write_dataset_org_lookup(self, dataset_lut_fname, org_lut_fname):
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
            uuids = gmetardr.get_dataset_uuids(self._dataset_pth)
    
            # Query/save dataset information
            gbifapi.get_write_dataset_meta(dataset_lut_fname, uuids)
            self._log.info('Wrote dataset metadata to {}'.format(dataset_lut_fname))
            
        if os.path.exists(org_lut_fname):
            self._log.info('Output file {} exists!'.format(org_lut_fname))
        else:
            # --------------------------------------
            # Gather organization UUIDs from dataset metadata assembled above
            org_uuids = gmetardr.get_organization_uuids(dataset_lut_fname)
            # Query/save organization information
            gbifapi.get_write_org_meta(org_lut_fname, org_uuids)
            self._log.info('Wrote organization metadata to {}'.format(org_lut_fname))
            
            
    # ...............................................
    def _append_resolved_taxkeys(self, lut, lut_fname, name_fails, nametaxa):
        """
        @summary: Create lookup table for: 
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        csvwriter, f = getCSVWriter(lut_fname, BISON_DELIMITER, ENCODING, 
                                    fmode='a')
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
    def _write_parsed_names(self, lut_fname, namelst):
        tot = 1000
        name_lut = {}
        name_fails = []
        csvwriter, f = getCSVWriter(lut_fname, BISON_DELIMITER, ENCODING, 
                                    fmode='w')
        header = ['provided_scientific_name_or_taxon_key', 'clean_provided_scientific_name']
        csvwriter.writerow(header)
        gbifapi = GbifAPI()
        while namelst:
            # Write first 1000, then delete first 1000
            currnames = namelst[:tot]
            namelst = namelst[tot:]
            # Get, write parsed names
            parsed_names, currfail = gbifapi.get_parsednames(currnames, lut_fname)
            name_fails.extend(currfail)
            for sciname, canonical in parsed_names.items():
                name_lut[sciname] = canonical
                csvwriter.writerow([sciname, canonical])
            self._log.info('Wrote {} sciname/canonical pairs ({} failed) to {}'
                           .format(len(parsed_names), len(currfail), lut_fname))
        return name_lut, name_fails
            
            
    # ...............................................
    def resolve_write_name_lookup(self, nametaxa_fname, name_lut_fname):
        """
        @summary: Create lookup table for: 
                  key GBIF scientificName or taxonKey, value clean_provided_scientific_name
        """
        if not os.path.exists(nametaxa_fname):
            raise Exception('Input file {} missing!'.format(nametaxa_fname))
        if os.path.exists(name_lut_fname):
            raise Exception('Output LUT file {} exists!'.format(name_lut_fname))
        
        # Read name/taxonIDs dictionary for name resolution
        nametaxa = self.lut.read_lookup(nametaxa_fname, 'scientificName', 
                                        BISON_DELIMITER)
        # Create name LUT with messyname/canonical from GBIF parser and save to file
        name_lut, name_fails = self._write_parsed_names(name_lut_fname, list(nametaxa.keys()))        
        # Append taxonkeys/canonical from GBIF taxonkey webservice to name LUT and file
        self._append_resolved_taxkeys(name_lut, name_lut_fname, 
                                      name_fails, nametaxa)
        
        return name_lut
            
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
                              GBIF record and field filter/transform.
                              First pass of CSV records fills most bison fields.
                              Resource and Organization values are filled in
                              from lookup tables from Step1.
                              Names and UUIDs are saved in records for GBIF API 
                              resolution in Step 2.
                           2: Name lookup table assembly:
                              * Query GBIF parser + scientificName if available, 
                                or GBIF species API + taxonKey for BISON 
                                'clean_provided_scientific_name'.
                              Then fill clean_provided_scientific_name field 
                              with resolved values saved in name lookup tables.
                           3: ITIS name lookup table assembly, then fill ITIS
                              fields and calculate FIPS, county, state fields 
                              by georeferencing with USCounties shapefile.
                           4: Fill establishment Means field with values in 
                              NonNativesIndex20190912.txt and fill 
                              calculated_waterbody and MRGID fields by 
                              georeferencing with World_EEZ_v8_20140228_splitpolygons 
                              shapefile.
                        """)
    args = parser.parse_args()
    gbif_interp_file = args.gbif_occ_file
    step = args.step

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
    terr_geo_shpname = os.path.join(ancillary_path, 'USCounties/USCounties.shp')
    centroid_lut_fname = os.path.join(ancillary_path, 'bison_geography.csv')
    estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
    eez_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
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
    pass1_fname = os.path.join(tmppath, 'step1_initialbison_{}.csv'.format(gbif_basefname))
    pass2_fname = os.path.join(tmppath, 'step2_cleannames_{}.csv'.format(gbif_basefname))
    pass3_fname = os.path.join(tmppath, 'step3_itis_geo_estmeans_{}.csv'.format(gbif_basefname))
    
    if not os.path.exists(gbif_interp_file):
        raise Exception('Filename {} does not exist'.format(gbif_interp_file))
    else:
        gr = GBIFReader(inpath, tmpdir, outdir, logbasename)
        if step == 1:
            gr.write_dataset_org_lookup(dataset_lut_fname, org_lut_fname)
            # Pass 1 of CSV transform, initial pull, standardize, 
            # FillMethod = gbif_meta, metadata fill
            gr.transform_gbif_to_bison(gbif_interp_file, dataset_lut_fname, 
                                       org_lut_fname, nametaxa_fname, pass1_fname)
            
        elif step == 2:
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(nametaxa_fname):
                gr.gather_name_input(pass1_fname, nametaxa_fname)
                
            name_lut = gr.resolve_write_name_lookup(nametaxa_fname, 
                                                    name_lut_fname)
            # Pass 2 of CSV transform
            # FillMethod = gbif_name, canonical name fill 
            gr.update_bison_names(pass1_fname, pass2_fname, name_lut, 
                                  discard_fields=['taxonKey'])
            
        elif step == 3:
            # Pass 3 of CSV transform
            # FillMethod = itis_tsn, georef (terrestrial)
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            gr.update_itis_geo_estmeans(pass2_fname, itis2_lut_fname, 
                                     terr_geo_shpname, centroid_lut_fname, 
                                     eez_shpname, estmeans_fname, pass3_fname)
"""
wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv

python3.6 /state/partition1/git/bison/src/gbif/gbif2bison.py 

import os
from osgeo import ogr 
import time

from gbif.constants import (GBIF_DELIMITER, BISON_DELIMITER, PROHIBITED_VALS, 
                            TERM_CONVERT, ENCODING, META_FNAME,
                            BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS,
                            CLIP_CHAR, FillMethod,GBIF_UUID_KEY)
from gbif.metareader import GBIFMetaReader
from common.tools import (getCSVReader, getCSVDictReader, 
                        getCSVWriter, getCSVDictWriter, getLine, getLogger)
from gbif.gbifapi import GbifAPI
from pympler import asizeof

ENCODING = 'utf-8'
BISON_DELIMITER = '$'


"""
