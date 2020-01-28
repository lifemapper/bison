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
import rtree
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_CHARS, PROHIBITED_SNAME_CHARS, 
                              PROHIBITED_VALS, ANCILLARY_FILES,
                              BISON_VALUES, BISON_SQUID_FLD, ITIS_KINGDOMS, 
                              ISO_COUNTRY_CODES)
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
    def __init__(self, infname, log=None):
        """
        @summary: Constructor
        """
        self.infname = infname
        pth, _ = os.path.split(infname)
        
        if not log:
            nm, _ = os.path.splitext(os.path.basename(__file__))
            logname = '{}.{}'.format(nm, int(time.time()))
            logfname = os.path.join(pth, '{}.log'.format(logname))
            log = getLogger(logname, logfname)
        self._log = log
        
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
    def _read_centroid_lookup(self, terrestrial_shpname):
        '''
        @summary: Read and populate dictionary with key = concatenated string of 
                  state name, county name, fips code and value = 
                  tuple of centroid longitude and latitude.
        '''
        driver = ogr.GetDriverByName("ESRI Shapefile")
        terr_data_src = driver.Open(terrestrial_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        terr_def = terrlyr.GetLayerDefn()
        idx_fips = terr_def.GetFieldIndex('FIPS')
        idx_cnty = terr_def.GetFieldIndex('COUNTY_NAM')
        idx_st = terr_def.GetFieldIndex('STATE_NAME')
        idx_centroid = terr_def.GetFieldIndex('centroid')

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
        terrlyr.ResetReading()
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
    def _open_files(self, infname, outfname=None):
        '''
        @summary: Open BISON-created CSV data for reading, 
                  new output file for writing
        '''
        # Open incomplete BISON CSV file as input
        self._log.info('Open input file {}'.format(infname))
        drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
        self._files.append(inf)
        # Optional new BISON CSV output file  
        dwtr = None
        if outfname:
            deleteme = []
            for fld in self._bison_ordered_flds:
                if fld not in drdr.fieldnames:
                    deleteme.append(fld)
            for fld in deleteme:
                self._bison_ordered_flds.remove(fld)
            self._log.info('Open output file {}'.format(outfname))
            dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
                                          self._bison_ordered_flds)
            dwtr.writeheader()
            self._files.append(outf)
        return drdr, dwtr
            
    # ...............................................
    def _fill_geofields(self, rec, lon, lat, 
                        terrindex, terrfeats, terr_bison_fldnames,
                        marindex, marfeats, mar_bison_fldnames):
        fldvals = {}
        terr_count = 0
        marine_count = 0
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(lon, lat)
        
        for tfid in list(terrindex.intersection((lon, lat))):
    #         feat = terrfeats[tfid]['feature']
    #         geom = feat.GetGeometryRef()
            geom = terrfeats[tfid]['geom']
            if pt.Within(geom):
                if terr_count == 0:
                    terr_count += 1
                    for fn in terr_bison_fldnames:
                        fldvals[fn] = terrfeats[tfid][fn]
                else:
                    terr_count = 0
                    fldvals = {}
                    break     
        # Single terrestrial polygon takes precedence
        if terr_count == 0:
            for mfid in list(marindex.intersection((lon, lat))):
                if marine_count == 0:
                    marine_count += 1
                    for fn in mar_bison_fldnames:
                        fldvals[fn] = marfeats[mfid][fn]
                else:
                    marine_count = 0
                    fldvals = {}
                    break
        # Update record with resolved values for intersecting polygons
        for name, val in fldvals.items():
            rec[name] = val

    # ...............................................
    def _fill_centroids(self, rec, centroids):
        pfips = rec['provided_fips']
        pcounty = rec['provided_county_name']
        pstate = rec['provided_state_name']
        if ((pcounty not in (None, '') and pstate not in (None, '')) 
            or pfips not in (None, '')):
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
            rec['hierarchy_string'] = itis_vals['hierarchy_string']
            rec['valid_accepted_scientific_name'] = itis_vals['valid_accepted_scientific_name']
            rec['valid_accepted_tsn'] = itis_vals['valid_accepted_tsn']
            rec['itis_common_name'] = itis_vals['common_name']
            if not rec['kingdom']:
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
    def update_itis_estmeans_centroid(self, itis2_lut_fname, estmeans_fname, 
                                      terrestrial_shpname, outfname, 
                                      fromGbif=True):
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
        # Derek's LUT header
        # scientific_name, tsn, valid_accepted_scientific_name, valid_accepted_tsn,
        # hierarchy_string, common_name, amb, kingdom
        itistsns = Lookup.initFromFile(itis2_lut_fname, 'scientific_name', ',', 
                                       valtype=VAL_TYPE.DICT, encoding=ENCODING)
        estmeans = self._read_estmeans_lookup(estmeans_fname)
        centroids = self._read_centroid_lookup(terrestrial_shpname)

        recno = 0
        try:
            dreader, dwriter = self._open_files(self.infname, outfname=outfname)
            for rec in dreader:
                recno += 1
                squid = rec[BISON_SQUID_FLD]
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
                # Fill missing coordinates or 
                #   refill previously computed to county centroid
                if lon is None or (centroid and centroid == 'county'):
                    self._fill_centroids(rec, centroids)
                    lon, lat = self._get_coords(rec)

                if not fromGbif:
                    self._fill_bison_provider_fields(rec)
                # Write updated record
                dwriter.writerow(rec)
                # Log progress occasionally
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(squid, recno, e))                    
        finally:
            self.close()
            
    # ...............................................    
    def _create_spatial_index(self, geodata, lyr):
        terr_def = lyr.GetLayerDefn()
        fldindexes = []
        bisonfldnames = []
        for geofld, bisonfld in geodata['fields']:
            geoidx = terr_def.GetFieldIndex(geofld)
            fldindexes.append((bisonfld, geoidx))
            bisonfldnames.append(bisonfld)
            
        spindex = rtree.index.Index(interleaved=False)
        spfeats = {}
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            geom = feat.GetGeometryRef()
            xmin, xmax, ymin, ymax = geom.GetEnvelope()
            spindex.insert(fid, (xmin, xmax, ymin, ymax))
            spfeats[fid] = {'feature': feat, 
                            'geom': geom}
            for name, idx in fldindexes:
                spfeats[fid][name] = feat.GetFieldAsString(idx)
        return spindex, spfeats, bisonfldnames

            
    # ...............................................
    def update_point_in_polygons(self, ancillary_path, outfname):
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

        terr_geodata = ANCILLARY_FILES['terrestrial']
        terrestrial_shpname = os.path.join(ancillary_path, terr_geodata['file'])
        terr_data_src = driver.Open(terrestrial_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        terrindex, terrfeats, terr_bison_fldnames = \
            self._create_spatial_index(terr_geodata, terrlyr)
        
        marine_geodata = ANCILLARY_FILES['marine']
        marine_shpname = os.path.join(ancillary_path, marine_geodata['file'])
        eez_data_src = driver.Open(marine_shpname, 0)
        eezlyr = eez_data_src.GetLayer()
        marindex, marfeats, mar_bison_fldnames = \
            self._create_spatial_index(marine_geodata, eezlyr)

        recno = 0
        try:
            dreader, dwriter = self._open_files(self.infname, outfname=outfname)
            for rec in dreader:
                recno += 1
                squid = rec[BISON_SQUID_FLD]
                lon, lat = self._get_coords(rec)
                # Use coordinates to calc 
                if lon is not None:
                    # Compute geo: coordinates and polygons
                    self._fill_geofields(rec, lon, lat, 
                                         terrindex, terrfeats, terr_bison_fldnames,
                                         marindex, marfeats, mar_bison_fldnames)
                # Write updated record
                dwriter.writerow(rec)
                # Log progress occasionally, this process is very time-consuming
                # so show progress at shorter intervals to ensure it is moving
                if (recno % (LOGINTERVAL/10)) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(squid, recno, e))                    
        finally:
            self.close()
            
            
    # ...............................................
    def _test_other_fields(self, rec, recno, squid):
        fips = rec['provided_fips']
        if fips != '' and len(fips) != 5:
            self._log.error('Record {}, {}, has invalid fips field {}'
                            .format(recno, squid, fips))
        king = rec['kingdom']
        if king and king.lower() not in ITIS_KINGDOMS:
            self._log.error('Record {}, {}, has invalid kingdom field {}'
                            .format(recno, squid, king))

            
    # ...............................................
    def _test_bison_provider_dependent_fields(self, rec, recno, squid):
        # Both required
        if rec['itis_tsn']:
            if (not rec['valid_accepted_scientific_name'] or 
                not rec['valid_accepted_tsn']):
                self._log.error("""Record {}, {}, has itis_tsn but missing
                valid_accepted_scientific_name or valid_accepted_tsn"""
                .format(recno, squid))
        lat = rec['latitude'] 
        lon = rec['longitude']
        # Neither or both
        if not ((lat and lon) or (not lat and not lon)):
            self._log.error('Record {}, {}, has only one of latitude or longitude'
            .format(recno, squid))
        # Centroid indicates calculated lat/lon
        elif rec['centroid'] and (not lat or not lon):
            self._log.error('Record {}, {}, has centroid but missing lat or long'
            .format(recno, squid))
        # negative longitude (x), positive latitude (y)
        elif (rec['iso_country_code'] in ('US', 'CA') and lat and lon):
            if float(lon) > 0 or float(lat) < 0:
                self._log.error("""Record {}, {}, from US or CA does not have 
                negative longitude and positive latitude"""
                .format(recno, squid))
        # Neither or both
        if rec['thumb_url'] and not rec['associated_media']:
            self._log.error('Record {}, {}, has thumb_url but missing associated_media'
            .format(recno, squid))
            
    # ...............................................
    def _test_bison_provider_fields(self, rec, recno, squid):
        for key, val in BISON_VALUES:
            if rec[key] != val:
                self._log.error('Editing Record {}, {}, from bad BISON {} value {} to {}'
                                .format(recno, squid, key, rec[key], val))
                rec[key] = val
        king = rec['kingdom']
        if king: 
            if not king.lower() in ITIS_KINGDOMS:
                self._log.error('Record {}, {}, has bad kingdom value {}'
                                .format(recno, squid, king))
            elif king != king.capitalize():
                self._log.error('Record {}, {}, has non-capitalized kingdom {}'
                                .format(recno, squid, king))
        ctry = rec['iso_country_code']
        if ctry and ctry not in ISO_COUNTRY_CODES:
            self._log.error('Record {}, {}, has invalid iso_country_code {}'
                            .format(recno, squid, ctry))
        
    # ...............................................
    def _fill_bison_provider_fields(self, rec):
        for key, val in BISON_VALUES:
            rec[key] = val
            
        king = rec['kingdom']
        if king and king.lower() in ITIS_KINGDOMS:
            rec['kingdom'] = king.capitalize()
        
    # ...............................................
    def _test_name(self, rec, recno, squid, bad_name_chars):
        cpsn = rec['clean_provided_scientific_name']
        psn = rec['provided_scientific_name']
        # Both required
        if not psn:
            self._log.error('Record {}, {}, has missing provided_scientific_name field {}'
                            .format(recno, squid))
        if not cpsn:
            self._log.error('Record {}, {}, has missing clean_provided_scientific_name field {}'
                            .format(recno, squid))
        # No bad chars, or leading, trailing, double spaces
        else:
            didx = cpsn.find('  ')
            trm_cpsn = cpsn.strip()
            if len(trm_cpsn) < cpsn:
                self._log.error('Record {}, {}, has leading or trailing blank chars in clean_provided_scientific_name {}'
                                .format(recno, squid, cpsn))
            elif didx >= 0:
                self._log.error('Record {}, {}, has double spaces in clean_provided_scientific_name {}'
                                .format(recno, squid, cpsn))
            else:
                for ch in cpsn:
                    if ch in bad_name_chars or ch.isdigit():
                        self._log.error('Record {}, {}, has prohibited characters in clean_provided_scientific_name {}'
                                        .format(recno, squid, cpsn))
                        break
                        
    # ...............................................
    def _test_dates(self, rec, recno, squid, currdate=(2020, 1, 1)):
        yr = rec['year']
        odate = rec['occurrence_date']
        if odate:
            parts = odate.split('-')
            try:
                for i in range(len(parts)):
                    int(parts[i])
            except:
                self._log.error('Record {}, {}, has non-date value'
                                .format(recno, squid, odate))
            else:
                if len(parts) in (1, 3):
                    if len(parts) == 3:
                        interpyr = parts[2]
                    elif len(parts) == 1:
                        interpyr = parts[0]
                    # Field dependency
                    if not yr:
                        self._log.error('Record {}, {}, has valid occurrence_date {} but no year'
                            .format(recno, squid, odate))
                    elif interpyr != yr:
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
        for key in rec:
            if key not in test_elsewhere:
                val = rec[key]
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
            
#         bad_res_chars = PROHIBITED_CHARS.copy()
#         bad_res_chars.extend(PROHIBITED_RESOURCE_CHARS)
        
        recno = 0
        try:
            dreader, _ = self._open_files(self.infname)
            for rec in dreader:
                recno += 1
                squid = rec[BISON_SQUID_FLD]
                
                # ..........................................
                # Test GBIF and bison provider datasets
                self._test_most_fields(rec, recno, squid)
                self._test_other_fields(rec, recno, squid)
                self._test_name(rec, recno, squid, bad_name_chars)
                self._test_dates(rec, recno, squid)
                
                # ..........................................
                # Test bison provider datasets
                if not fromGbif:
                    self._test_bison_provider_fields(rec, recno, squid)
                    self._test_bison_provider_dependent_fields(rec, recno, squid)
                    
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
    estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
    itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')
    terrestrial_shpname = os.path.join(ancillary_path, 
                                       ANCILLARY_FILES['terrestrial']['file'])
    marine_shpname = os.path.join(ancillary_path, 
                                  ANCILLARY_FILES['marine']['file'])
    
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
