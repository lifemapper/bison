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

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_CHARS, PROHIBITED_SNAME_CHARS, 
                              PROHIBITED_RESOURCE_CHARS, PROHIBITED_VALS,
                              BISON_VALUES, ITIS_KINGDOMS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVDictReader, getCSVDictWriter, getLogger)

from gbif.constants import (BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS, 
                            DISCARD_AFTER_UPDATE)


# .............................................................................
class BisonFiller(object):
    """
    Object for processing a CSV file with 47 ordered BISON fields (and optional
    gbifID field for GBIF provided data) filling ITIS, coordinates, terrestrial
    or marine boundaries, and establishment_means.
    """
    # ...............................................
    def __init__(self, infname, outfname):
        """
        @summary: Constructor
        """
        self.infname = infname
        self.outfname = outfname
        pth, _ = os.path.split(infname)
        
        nm, _ = os.path.splitext(os.path.basename(__file__))
        logname = '{}.{}'.format(nm, int(time.time()))
        logfname = os.path.join(pth, '{}.log'.format(logname))
        self._log = getLogger(logname, logfname)

        # Ordered output fields
        self._bison_ordered_flds = []
        discards = DISCARD_FIELDS.copy().extend(DISCARD_AFTER_UPDATE)
        
        for (bisonfld, _) in BISON_GBIF_MAP:
            if bisonfld not in DISCARD_FIELDS:
                self._bison_ordered_flds.append(bisonfld)

        self._files = []
                
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
    def _open_files(self, infname, outfname):
        '''
        @summary: Open BISON-created CSV data for reading, 
                  new output file for writing
        '''
        # Open incomplete BISON CSV file as input
        self._log.info('Open input file {}'.format(infname))
        drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
        self._files.append(inf) 
        
        # Open new BISON CSV file for output
        self._log.info('Open output file {}'.format(outfname))
        dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
                                      self._bison_ordered_flds)
        dwtr.writeheader()
        self._files.append(outf)
        return drdr, dwtr
            
    # ...............................................
    def _fill_geofields(self, rec, lon, lat,
                        terrlyr, idx_fips, idx_cnty, idx_st, 
                        eezlyr, idx_eez, idx_mg):
        terr_found_one = False
        marine_found_one = False
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(lon, lat)
        terrlyr.SetSpatialFilter(pt)
        eezlyr.SetSpatialFilter(pt)
        for poly in terrlyr:
            if not terr_found_one:
                terr_found_one = True
                fips = poly.GetFieldAsString(idx_fips)
                county = poly.GetFieldAsString(idx_cnty)
                state = poly.GetFieldAsString(idx_st)
            else:
                terr_found_one = False
                break
        # Single terrestrial polygon takes precedence
        if terr_found_one:
            # terrestrial intersect
            rec['calculated_fips'] = fips
            rec['calculated_county_name'] = county
            rec['calculated_state_name'] = state
        # Single marine intersect is 2nd choice
        else:
            for poly in eezlyr:
                if not marine_found_one:
                    marine_found_one = True
                    eez = poly.GetFieldAsString(idx_eez)
                    mrgid = poly.GetFieldAsString(idx_mg)
                else:
                    marine_found_one = False
                    break
            if marine_found_one:
                rec['calculated_waterbody'] = eez
                rec['mrgid'] = mrgid

    # ...............................................
    def _fill_centroids(self, rec, centroids):
        pfips = rec['provided_fips']
        pcounty = rec['provided_county_name']
        pstate = rec['provided_state_name']
        if ((pcounty not in (None, '') and pstate not in (None, '')) 
            or pfips not in (None, '')):
#             self._log.info('Provided county {}, state {}, fips {}'
#                            .format(pcounty, pstate, pfips))
            key = ';'.join((pstate, pcounty, pfips))
            try:
                lon, lat = centroids.lut[key]
            except:
                pass
            else:
                rec['longitude'] = lon
                rec['latitude'] = lat
                rec['centroid'] = 'county'

#     # ...............................................
#     def _get_itisfields(self, name, itis_svc):
#         # Get tsn, acceptedTSN, accepted_name, kingdom
#         accepted_tsn = row = None
#         tsn, accepted_name, kingdom, accepted_tsn_list = itis_svc.get_itis_tsn(name)
#         if accepted_name is None:
#             for accepted_tsn in accepted_tsn_list:
#                 accepted_name, kingdom = itis_svc.get_itis_name(accepted_tsn)
#                 if accepted_name is not None:
#                     break
#         else:
#             accepted_tsn = tsn
#         
#         for v in (tsn, accepted_name, kingdom, accepted_tsn):
#             if v is not None:
#                 row = [tsn, accepted_name, kingdom, accepted_tsn]
#                 break
#         if row:
#             # Get common names
#             common_names = itis_svc.get_itis_vernacular(accepted_tsn)
#             common_names_str = ';'.join(common_names)
#             row.append(common_names_str)
#         return row
#     
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
#             

    # ...............................................
    def _fill_itisfields(self, rec, itistsns):
        """
        Derek-provided LUT header:
        scientific_name, tsn, valid_accepted_scientific_name, valid_accepted_tsn,
        hierarchy_string, common_name, amb
        """
        canonical = rec['clean_provided_scientific_name']
        try:
            itis_vals = itistsns.lut[canonical]
        except Exception as e:
            pass
#             self._log.info('Found NO itis values for {}'.format(canonical))                    
        else:
            rec['itis_tsn'] = itis_vals['tsn']
            rec['valid_accepted_scientific_name'] = itis_vals['valid_accepted_scientific_name']
            rec['valid_accepted_tsn'] = itis_vals['valid_accepted_tsn']
            rec['itis_common_name'] = itis_vals['common_name']
            if rec['kingdom'] is None:
                rec['kingdom'] = itis_vals['kingdom']

    # ...............................................
    def _fill_estmeans_field(self, rec, estmeans):
        em = None
        tsn = rec['itis_tsn']
        sname = rec['clean_provided_scientific_name']
        try:
            em = estmeans.lut[tsn]
        except:
            try:
                em = estmeans.lut[sname]
            except:
                pass
        rec['establishment_means'] = em

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
    def update_itis_geo_estmeans(self, itis2_lut_fname, terrestrial_shpname,  
                                 marine_shpname, estmeans_fname):
        """
        @summary: Process a CSV file with 47 ordered BISON fields (and optional
                  gbifID field for GBIF provided data) to 
                  1) fill itis_tsn, valid_accepted_scientific_name, 
                     valid_accepted_tsn, itis_common_name, kingdom (if blank) 
                     with ITIS values based on clean_provided_scientific_name, 
                  2) georeference records without coordinates or re-georeference 
                     records previously georeferenced to county centroid, 
                  3) fill terrestrial (state/county/fips) or marine (EEZ) fields
                     for reported/computed coordinates
                  4) fill 'establishment_means' field for species non-native 
                     to Alaska, Hawaii, or Lower 48 based on itis_tsn or 
                     clean_provided_scientific_name
        @return: A CSV file of BISON-modified records  
        """
        if self.is_open():
            self.close()
        driver = ogr.GetDriverByName("ESRI Shapefile")
        terr_data_src = driver.Open(terrestrial_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        terr_def = terrlyr.GetLayerDefn()
        idx_fips = terr_def.GetFieldIndex('FIPS')
        idx_cnty = terr_def.GetFieldIndex('COUNTY_NAM')
        idx_st = terr_def.GetFieldIndex('STATE_NAME')
        idx_centroid = terr_def.GetFieldIndex('centroid')
        
        eez_data_src = driver.Open(marine_shpname, 0)
        eezlyr = eez_data_src.GetLayer()
        eez_def = eezlyr.GetLayerDefn()
        idx_eez = eez_def.GetFieldIndex('EEZ')
        idx_mg = eez_def.GetFieldIndex('MRGID')

        # Derek's LUT header
        # scientific_name, tsn, valid_accepted_scientific_name, valid_accepted_tsn,
        # hierarchy_string, common_name, amb, kingdom
        itistsns = Lookup.initFromFile(itis2_lut_fname, 'scientific_name', ',', 
                                       valtype=VAL_TYPE.DICT, encoding=ENCODING)
        estmeans = self._read_estmeans_lookup(estmeans_fname)
        centroids = self._read_centroid_lookup(terrlyr, idx_fips, idx_cnty, 
                                               idx_st, idx_centroid)

        recno = 0
        try:
            dreader, dwriter = self._open_files(self.infname, self.outfname)
            for rec in dreader:
                recno += 1
                gid = rec[OCC_UUID_FLD]
                # ..........................................
                # Fill ITIS 
                self._fill_itisfields(rec, itistsns)
                # ..........................................
                # Fill establishment_means from TSN or 
                # clean_provided_scientific_name and establishment means table
                self._fill_estmeans_field(rec, estmeans)
                # ..........................................
                # Fill geo
                centroid = rec['centroid']
                lon, lat = self._get_coords(rec)
                # Fill coordinates if missing
                if lon is None or (centroid is not None and centroid == 'county'):
                    self._fill_centroids(rec, centroids)
                    lon, lat = self._get_coords(rec)
                # Use coordinates to calc 
                if lon is not None:
                    # Compute geo: coordinates and polygons
                    self._fill_geofields(rec, lon, lat, 
                                         terrlyr, idx_fips, idx_cnty, idx_st, 
                                         eezlyr, idx_eez, idx_mg)
                # Write updated record
                dwriter.writerow(rec)
                # Log progress occasionally
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(gid, recno, e))                    
        finally:
            self.close()
            
            
    # ...............................................
    def _test_other_fields(self, rec, recno, squid):
        fips = rec['provided_fips']
        if fips != '' and len(fips) != 5:
            self._log.error('Record {}, {}, has invalid fips field {}'
                            .format(recno, squid, fips))
        # TODO: Capitalize in bison provider data
        king = rec['kingdom']
        if king != '' and king.lower() not in ITIS_KINGDOMS:
            self._log.error('Record {}, {}, has invalid kingdom field {}'
                            .format(recno, squid, king))

            
    # ...............................................
    def _test_bison_provider_fields(self, rec, recno, squid):
        for key, val in BISON_VALUES:
            if rec[key] != val:
                self._log.error('Record {}, {}, has bad BISON provider {} value {}'
                                .format(recno, squid, key, val))
        if rec['clean_provided_scientific_name'] == '':
            self._log.error('Record {}, {}, has missing clean_provided_scientific_name field {}'
                            .format(recno, squid))
        if rec['provided_scientific_name'] == '':
            self._log.error('Record {}, {}, has missing provided_scientific_name field {}'
                            .format(recno, squid))
        

    # ...............................................
    def _test_name(self, rec, recno, squid, bad_name_chars):
        sname = rec['clean_provided_scientific_name']
        didx = sname.find('  ')
        if sname is None or sname == '':
            self._log.error('Record {}, {}, has blank clean_provided_scientific_name'
                            .format(recno, squid))
        else:
            ssname = sname.strip()
            if len(ssname) < sname:
                self._log.error('Record {}, {}, has leading or trailing blank chars in clean_provided_scientific_name {}'
                                .format(recno, squid, sname))
            elif didx >= 0:
                self._log.error('Record {}, {}, has double spaces in clean_provided_scientific_name {}'
                                .format(recno, squid, sname))
            else:
                for ch in sname:
                    if ch in bad_name_chars:
                        self._log.error('Record {}, {}, has prohibited characters in clean_provided_scientific_name {}'
                                        .format(recno, squid, sname))
                        break
                    if ch.isdigit():
                        self._log.error('Record {}, {}, has numeral(s) in clean_provided_scientific_name {}'
                                        .format(recno, squid, sname))
                        break
                        
    # ...............................................
    def _test_dates(self, rec, recno, squid, currdate=(2020, 1, 1)):
        yr = rec['year']
        odate = rec['occurrence_date']
        parts = odate.split('-')
        try:
            for i in range(len(parts)):
                int(parts[i])
        except:
            self._log.error('Record {}, {}, has non-date value'
                            .format(recno, squid, odate))
        if len(parts) in (1, 3):
            if len(parts) == 3:
                interpyr = parts[2]
            elif len(parts) == 1:
                interpyr = parts[0]
            if interpyr != yr:
                self._log.error('Record {}, {}, has non-matching date {} and year {}'
                                .format(recno, squid, odate, yr))
        else:
            self._log.error('Record {}, {}, has non- simple-date format'
                            .format(recno, squid, odate))
            
        if yr is not None and yr != '':
            try:
                year = int(yr)
                if year < 1500 or year > currdate[0]:
                    self._log.error('Record {}, {}, has low year {}'
                                    .format(recno, squid, yr))
            except:
                    self._log.error('Record {}, {}, has invalid year {}'
                        .format(recno, squid, yr))
                
        
    # ...............................................
    def _test_most_fields(self, rec, recno, squid):
        test_elsewhere = ['clean_provided_scientific_name', 'occurrence_date',
                          'year', 'resource', 'provided_fips', 'kingdom']
        count = len(rec)            
        if count != 47:
            self._log.error('Record {}, {}, has {} fields '
                            .format(recno, squid, count))
        # Invalid chars
        for (key, val) in rec.iteritems():
            if key not in test_elsewhere:
                if val in PROHIBITED_VALS:
                    self._log.error('Record {}, {}, field {}, value {} is prohibited'
                                    .format(recno, squid, key, val))
                else:
                    for ch in PROHIBITED_CHARS:
                        idx = val.find(ch)
                        if idx >= 0:
                            self._log.error('Record {}, {}, field {}, value {} has prohibited char'
                                            .format(recno, squid, key, val))

    # ...............................................
    def test_bison_outfile(self, fromGbif=True):
        """
        @summary: Process a CSV file with 47 ordered BISON fields (and optional
                  gbifID field for GBIF provided data) to test for data correctness
        @return: A file of BISON-modified records  
        """
        if self.is_open():
            self.close()
            
        bad_name_chars = PROHIBITED_CHARS.copy()
        bad_name_chars.extend(PROHIBITED_SNAME_CHARS)
            
        bad_res_chars = PROHIBITED_CHARS.copy()
        bad_res_chars.extend(PROHIBITED_RESOURCE_CHARS)
        
        recno = 0
        try:
            dreader, dwriter = self._open_files(self.infname, self.outfname)
            for rec in dreader:
                recno += 1
                squid = rec['occurrence_url']
                self._test_most_fields(rec, recno, squid)
                
                self._test_name(rec, recno, squid, bad_name_chars)
                self._test_dates(rec, recno, squid)
                if not fromGbif:
                    self._test_bison_provider_fields(rec, recno, squid)

                # ..........................................
                # Log progress occasionally
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(squid, recno, e))                    
        finally:
            self.close()
            
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Parse a BISON occurrence dataset partially 
                                processed from GBIF or a BISON provider.
                                Fill fields:
                                - ITIS fields - resolve with ITIS lookup and
                                  clean_provided_scientific_name filled in step2
                                - establishment_means - resolve with establishment
                                  means lookup and ITIS TSN or 
                                  clean_provided_scientific_name
                                - geo fields - resolve with (1st) terrestrial 
                                  shapefile US_CA_Counties_Centroids 
                                  or (2nd) marine shapefile 
                                  World_EEZ_v8_20140228_splitpolygons
                                  - lon/lat and centroid for records without
                                    lon/lat or previously computed county centroid 
                                    and with state+county or fips values
                                    from terrestrial centroid coordinates
                                  - calculated state, county, FIPS, county  
                                    fields for records with new or existing 
                                    lon/lat 
                                  - EEZ and mrgid (marine) for records with 
                                    lon/lat - only if terrestrial georef returns 
                                    nothing
                                 """))
    parser.add_argument('infile', type=str, 
                        help="""
                        Absolute pathname of the input BISON occurrence file 
                        to supplement.  
                        """)
    parser.add_argument('outfile', type=str, 
                        help="""
                        Absolute pathname of the output BISON occurrence file 
                        """)
    parser.add_argument('ancillary_path', type=str, 
                        help="""
                        Absolute pathname of the directory with supplemental 
                        lookup and geospatial files.
                        """)
    args = parser.parse_args()
    infile = args.infile
    outfile = args.outfile
    ancillary_path = args.ancillary_path
    if not os.path.exists(infile):
        raise Exception('Input file {} does not exist'.format(infile))
    if os.path.exists(outfile):
        raise Exception('Output file {} already exists'.format(outfile))
    if not os.path.exists(ancillary_path):
        raise Exception('Ancillary path {} does not exist'.format(ancillary_path))

    overwrite = True
    outpath = os.path.split(outfile)
    os.makedirs(outpath, mode=0o775, exist_ok=True)
    
    # ancillary data for record update
    terrestrial_shpname = os.path.join(ancillary_path, 'US_CA_Counties_Centroids.shp')
    estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
    marine_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
    itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')
    
#     itis1_lut_fname = os.path.join(tmppath, 'step3_itis_lut.txt')
    
    logbasename = 'bisonfill_{}'.format()
#     pass3_fname = os.path.join(tmppath, 'step3_itis_geo_estmeans_{}.csv'.format(gbif_basefname))
    
    gr = BisonFiller(infile, outfile)            
    # Pass 3 of CSV transform
    # FillMethod = itis_tsn, georef (terrestrial)
    # Use Derek D. generated ITIS lookup itis2_lut_fname
    gr.update_itis_geo_estmeans(itis2_lut_fname, terrestrial_shpname, 
                                marine_shpname, estmeans_fname)
"""
wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv

python3.6 /state/partition1/git/bison/src/common/bisonfill.py 

import os
from osgeo import ogr 
import time

from gbif.constants import (GBIF_DELIMITER, BISON_DELIMITER, PROHIBITED_VALS, 
                            TERM_CONVERT, ENCODING, META_FNAME,
                            BISON_GBIF_MAP, OCC_UUID_FLD, DISCARD_FIELDS,
                            CLIP_CHAR, FillMethod,GBIF_UUID_KEY)
from gbif.gbifmeta import GBIFMetaReader
from common.tools import (getCSVReader, getCSVDictReader, 
                        getCSVWriter, getCSVDictWriter, getLine, getLogger)
from gbif.gbifapi import GbifAPI
from pympler import asizeof

ENCODING = 'utf-8'
BISON_DELIMITER = '$'


"""
