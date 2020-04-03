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
                              ISO_COUNTRY_CODES, BISON_ORDERED_DATALOAD_FIELDS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVDictReader, open_csv_files, getLogger)

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
        # Individual steps may add/remove temporary fields for input/output
        self._infields = BISON_ORDERED_DATALOAD_FIELDS.copy()
        # Write these fields after processing for next step
        self._outfields = BISON_ORDERED_DATALOAD_FIELDS.copy()
#         discards = DISCARD_FIELDS.copy().extend(DISCARD_AFTER_UPDATE)
#         
#         for (bisonfld, _) in BISON_GBIF_MAP:
#             if bisonfld not in DISCARD_FIELDS:
#                 self._bison_ordered_flds.append(bisonfld)

        self._files = []
                
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
        time.sleep(30)
        terrlyr = terr_data_src.GetLayer()
        terr_def = terrlyr.GetLayerDefn()
        idx_fips = terr_def.GetFieldIndex('B_FIPS')
        idx_cnty = terr_def.GetFieldIndex('B_COUNTY')
        idx_st = terr_def.GetFieldIndex('B_STATE')
        idx_centroid = terr_def.GetFieldIndex('B_CENTROID')

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
    def _fill_geofields(self, rec, lon, lat, 
                        terrindex, terrfeats, terr_bison_fldnames,
                        marindex, marfeats, mar_bison_fldnames):
        fldvals = {}
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(lon, lat)
        
#         self._log.info('**** Start geo-intersect')
        start = time.time()
        terr_intersect_fids = list(terrindex.intersection((lon, lat)))
        self._log.info('Rtree intersect time for {} matching fids: {}'.format(
            len(terr_intersect_fids), time.time()-start))
        
        terr_count = 0
        start = time.time()
        for tfid in terr_intersect_fids:
            geom = terrfeats[tfid]['geom']
            start = time.time()
            is_within = pt.Within(geom)
#             self._log.info('  ogr within time: {}'.format(time.time()-start))
            if is_within:
                terr_count += 1
                # If intersects, take values for first polygon
                if terr_count == 1:
                    for fn in terr_bison_fldnames:
                        fldvals[fn] = terrfeats[tfid][fn]
                # If > 1 polygon, clear all values
                else:
                    self._log.info('  FOUND AGAIN!')
                    fldvals = {}
                    break
        self._log.info('  OGR within time {}'.format(time.time()-start))
        
#         if terr_count != 1:
#             mar_intersect_fids = list(marindex.intersection((lon, lat)))
#             marine_count = 0
#             for mfid in mar_intersect_fids:
#                 geom = marfeats[mfid]['geom']
#                 if pt.Within(geom):
#                     marine_count += 1
#                     # If intersects, take values for first polygon
#                     if marine_count == 1:
#                         for fn in mar_bison_fldnames:
#                             fldvals[fn] = marfeats[mfid][fn]
#                     # If > 1 polygon, clear marine values (leave terr)
#                     else:
#                         for fn in mar_bison_fldnames:
#                             fldvals[fn] = None
#                         break
#             stop = time.time()
#             elapsed = stop - start
#             self._log.info('Time for intersect {}'.format(elapsed))

        # Update record with resolved values for intersecting polygons
        for name, val in fldvals.items():
            rec[name] = val
        return terr_count

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
                self._log.info('Missing county centroid for {}'.format(key))
            else:
                rec['longitude'] = lon
                rec['latitude'] = lat
                rec['centroid'] = 'county'
                self._log.info('Filled county centroid {}, {}'.format(lon, lat))
        # No lon/lat and no fips or state/county 
        else:
            rec = None
            self._log.info('No info for county centroid {}, {}, {}'.format(
                pfips, pcounty, pstate))

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
    def _clean_kingdom(self, rec):
        kingdom = rec['kingdom']
        if kingdom is not None:
            if kingdom.lower() not in ITIS_KINGDOMS:
                rec['kingdom'] = None
            else:
                rec['kingdom'] = kingdom.capitalize()

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
        else:
            rec['itis_tsn'] = itis_vals['tsn']
            rec['hierarchy_string'] = itis_vals['hierarchy_string']
            rec['amb'] = itis_vals['amb']
            rec['valid_accepted_scientific_name'] = itis_vals['valid_accepted_scientific_name']
            rec['valid_accepted_tsn'] = itis_vals['valid_accepted_tsn']
            rec['itis_common_name'] = itis_vals['common_name']
            # replace field with ITIS value
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
    def _fix_itis_kingdoms(self, itistsns):
        for key, vals in itistsns.lut.items():
            itistsns.lut[key]['kingdom'] = vals['kingdom'].strip()
        
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
        self._fix_itis_kingdoms(itistsns)
        estmeans = self._read_estmeans_lookup(estmeans_fname)
        centroids = self._read_centroid_lookup(terrestrial_shpname)

        recno = 0
        try:
            dict_reader, inf, writer, outf = open_csv_files(self.infname, 
                                                 BISON_DELIMITER, ENCODING, 
                                                 outfname=outfname, 
                                                 outfields=self._outfields)
                
            for rec in dict_reader:
                recno += 1
                squid = rec[BISON_SQUID_FLD]
                
                # ..........................................
                # Clean kingdom value, Fill ITIS
                self._clean_kingdom(rec) 
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

#                 if not fromGbif:
#                     self._fill_bison_provider_fields(rec)
                # Write updated record
                if lon is not None:
                    row = self._makerow(rec)
                    writer.writerow(row)
                # Log progress occasionally
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                                    
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(squid, recno, e))                    
        finally:
            inf.close()
            outf.close()
            
    # ...............................................    
    def _create_spatial_index(self, flddata, lyr):
        lyr_def = lyr.GetLayerDefn()
        fldindexes = []
        bisonfldnames = []
        for geofld, bisonfld in flddata:
            geoidx = lyr_def.GetFieldIndex(geofld)
            fldindexes.append((bisonfld, geoidx))
            bisonfldnames.append(bisonfld)
             
        spindex = rtree.index.Index(interleaved=False)
        spfeats = {}
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            geom = feat.geometry()
            xmin, xmax, ymin, ymax = geom.GetEnvelope()
            spindex.insert(fid, (xmin, xmax, ymin, ymax))
            spfeats[fid] = {'feature': feat, 
                            'geom': geom}
            for name, idx in fldindexes:
                spfeats[fid][name] = feat.GetFieldAsString(idx)
        return spindex, spfeats, bisonfldnames
# 
#     # ...............................................    
#     def _create_spatial_index(self, flddata, lyr):
#         bisonfldnames = []
#         for reffld, bisonfld in flddata:
#             bisonfldnames.append(bisonfld)
#             
#         spindex = rtree.index.Index(interleaved=False)
#         spfeats = {}
#         feat = lyr.GetNextFeature()
#         while feat is not None:
#             fid = feat.GetFID()
#             geom = feat.GetGeometryRef()
# #             self._log.info('  {} Geometry {} count {}'.format(
# #                 fid, geom.GetGeometryName(), geom.GetGeometryCount()))
#             # GetEnvelope returns ordering:  minX, maxX, minY, maxY
#             xmin, xmax, ymin, ymax = geom.GetEnvelope()
#             # Interleaved = False requires: minX, maxX, minY, maxY
#             spindex.insert(fid, (xmin, xmax, ymin, ymax))
#             spfeats[fid] = {'feature': feat, 
#                             'geom': geom}
#             for reffld, bisonfld in flddata:
#                 spfeats[fid][bisonfld] = feat.GetFieldAsString(reffld)
#             feat = lyr.GetNextFeature()
#         return spindex, spfeats, bisonfldnames

            
    # ...............................................
    def update_point_in_polygons(self, terr_data, marine_data, ancillary_path, 
                                 outfname):
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

        terrestrial_shpname = os.path.join(ancillary_path, terr_data['file'])
        terr_data_src = driver.Open(terrestrial_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        terrindex, terrfeats, terr_bison_fldnames = \
            self._create_spatial_index(terr_data['fields'], terrlyr)
        
        marine_shpname = os.path.join(ancillary_path, marine_data['file'])
        eez_data_src = driver.Open(marine_shpname, 0)
        eezlyr = eez_data_src.GetLayer()
        marindex, marfeats, mar_bison_fldnames = \
            self._create_spatial_index(marine_data['fields'], eezlyr)

        matches = {0: 0, 1: 0, 2: 0}
        recno = 0
        try:
            start_time = time.time()
            loop_time = start_time
            dict_reader, inf, writer, outf = open_csv_files(self.infname, 
                                             BISON_DELIMITER, ENCODING, 
                                             outfname=outfname, 
                                             outfields=self._outfields)
            for rec in dict_reader:
                recno += 1
                squid = rec[BISON_SQUID_FLD]
                lon, lat = self._get_coords(rec)
                # Use coordinates to calc 
                if lon is None:
                    print('wtf {}'.format(squid))
                else:
                    # Compute geo: coordinates and polygons
                    match_count = self._fill_geofields(rec, lon, lat, 
                                         terrindex, terrfeats, terr_bison_fldnames,
                                         marindex, marfeats, mar_bison_fldnames)
                    if match_count == 0:
                        matches[0] += 1
                    elif match_count == 1:
                        matches[1] += 1
                    else:
                        matches[2] += 1
                    # Write updated record
                    row = self._makerow(rec)
                    writer.writerow(row)
                # Log progress occasionally, this process is very time-consuming
                # so show progress at shorter intervals to ensure it is moving
                if (recno % 5000) == 0:
                    now = time.time()
                    self._log.info('*** Record number {}, elapsed {} ***'.format(
                        recno, now - loop_time))
                    loop_time = now
                
        except Exception as e:
            self._log.error('Failed filling data from id {}, line {}: {}'
                            .format(squid, recno, e))                    
        finally:
            inf.close()
            outf.close()
            self._log.info('*** Elapsed time {} for {} records ***'.format(
                time.time() - start_time, recno))
            self._log.info('*** Matched 0: {}, 1: {}, >1: {} records ***'.format(
                matches[0], matches[1], matches[2]))
            
            
    # ...............................................
    def test_point_in_polygons(self, ancillary_path, outfname):
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

        terr_data = ANCILLARY_FILES['terrestrial']
        terrestrial_shpname = os.path.join(ancillary_path, terr_data['file'])
        terr_data_src = driver.Open(terrestrial_shpname, 0)
        terrlyr = terr_data_src.GetLayer()
        self._log.info('*** Terrestrial layer {} with {} features'.format(
            terrestrial_shpname, terrlyr.GetFeatureCount()))
        terrindex, terrfeats, terr_bison_fldnames = \
            self._create_spatial_index(terr_data['fields'], terrlyr)
        
        marine_data = ANCILLARY_FILES['marine']
        marine_shpname = os.path.join(ancillary_path, marine_data['file'])
        eez_data_src = driver.Open(marine_shpname, 0)
        eezlyr = eez_data_src.GetLayer()
        self._log.info('*** Marine layer {} with {} features'.format(
            terrestrial_shpname, terrlyr.GetFeatureCount()))
        marindex, marfeats, mar_bison_fldnames = \
            self._create_spatial_index(marine_data['fields'], eezlyr)

        return  ((terr_data_src, terrlyr, terrindex, terrfeats, terr_bison_fldnames), 
                 (eez_data_src, eezlyr, marindex, marfeats, mar_bison_fldnames))

#         recno = 0
#         try:
#             dict_reader, inf, writer, outf = open_csv_files(self.infname, 
#                                              BISON_DELIMITER, ENCODING, 
#                                              outfname=outfname, 
#                                              outfields=self._outfields)
#             for rec in dict_reader:
#                 recno += 1
#                 squid = rec[BISON_SQUID_FLD]
#                 lon, lat = self._get_coords(rec)
#                 # Use coordinates to calc 
#                 if lon is not None:
#                     # Compute geo: coordinates and polygons
#                     self._fill_geofields(rec, lon, lat, 
#                                          terrindex, terrfeats, terr_bison_fldnames,
#                                          marindex, marfeats, mar_bison_fldnames)
#                 # Write updated record
#                 row = self._makerow(rec)
#                 writer.writerow(row)
#                 # Log progress occasionally, this process is very time-consuming
#                 # so show progress at shorter intervals to ensure it is moving
#                 if (recno % LOGINTERVAL/10) == 0:
#                     self._log.info('*** Record number {} ***'.format(recno))
#                                     
#         except Exception as e:
#             self._log.error('Failed filling data from id {}, line {}: {}'
#                             .format(squid, recno, e))                    
#         finally:
#             inf.close()
#             outf.close()
            
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
        
        recno = 0
        try:
            dict_reader, inf, _, _ = open_csv_files(self.infname, 
                                                    BISON_DELIMITER, ENCODING)
            for rec in dict_reader:
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
            inf.close()

#     # ...............................................
#     def write_dataset_org_lookup(self, dataset_lut_fname, resource_lut_fname, 
#                                  org_lut_fname, provider_lut_fname, 
#                                  outdelimiter=BISON_DELIMITER):
#         """
#         @summary: Create lookup table for: 
#                   BISON resource and provider from 
#                   GBIF datasetKey and dataset publishingOrganizationKey
#         @return: One file, containing dataset metadata, 
#                            including publishingOrganization metadata 
#                            for that dataset
#         """
#         gbifapi = GbifAPI()
#         if os.path.exists(dataset_lut_fname):
#             self._log.info('Output file {} exists!'.format(dataset_lut_fname))
#         else:
#             old_resources = Lookup.initFromFile(resource_lut_fname, GBIF_UUID_KEY, 
#                                             ANCILLARY_DELIMITER, valtype=VAL_TYPE.DICT, 
#                                             encoding=ENCODING)
#             datasets = self._write_dataset_lookup(gbifapi, old_resources,
#                                                   dataset_lut_fname, 
#                                                   outdelimiter)
#             
#         if os.path.exists(org_lut_fname):
#             self._log.info('Output file {} exists!'.format(org_lut_fname))
#         else:
#             old_providers = Lookup.initFromFile(provider_lut_fname, GBIF_UUID_KEY, 
#                                             ANCILLARY_DELIMITER, valtype=VAL_TYPE.DICT, 
#                                             encoding=ENCODING)
#     
#             # --------------------------------------
#             # Gather organization UUIDs from dataset metadata assembled (LUT or file)
#             org_uuids = set()
#             try:
#                 for key, ddict in datasets.lut.items():
#                     try:
#                         org_uuids.add(ddict['publishingOrganizationKey'])
#                     except Exception as e:
#                         print('No publishingOrganizationKey in dataset {}'.format(key))
#             except Exception as e:             
#                 gmetardr = GBIFMetaReader(self._log)
#                 org_uuids = gmetardr.get_organization_uuids(dataset_lut_fname)
#                 
#             self._write_org_lookup(org_uuids, gbifapi, old_providers, 
#                                    org_lut_fname, outdelimiter)
            
            
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
#     itis1_lut_fname = os.path.join(tmppath, 'step3_itis_lut.txt')
    itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')
    terrestrial_shpname = os.path.join(ancillary_path, 
                                       ANCILLARY_FILES['terrestrial']['file'])
    marine_shpname = os.path.join(ancillary_path, 
                                  ANCILLARY_FILES['marine']['file'])
        
    logbasename = 'bisonfill_{}'.format()
#     pass3_fname = os.path.join(tmppath, 'step3_itis_geo_estmeans_{}.csv'.format(gbif_basefname))
    
    gr = BisonFiller(infile, outfile)            
    # Pass 3 of CSV transform
    # Use Derek D. generated ITIS lookup itis2_lut_fname
    gr.update_itis_geo_estmeans(itis2_lut_fname, terrestrial_shpname, 
                                marine_shpname, estmeans_fname)
"""
wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv

python3.6 /state/partition1/git/bison/src/common/bisonfill.py 

from osgeo import ogr
import os
import rtree
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_CHARS, PROHIBITED_SNAME_CHARS, 
                              PROHIBITED_VALS, ANCILLARY_FILES,
                              BISON_VALUES, BISON_SQUID_FLD, ITIS_KINGDOMS, 
                              ISO_COUNTRY_CODES, BISON_ORDERED_DATALOAD_FIELDS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVDictReader, open_csv_files, getLogger)


self = BisonFiller(pass3_fname, log=logger)
# TEST - go to BisonFiller code

driver = ogr.GetDriverByName("ESRI Shapefile")

terr_data = ANCILLARY_FILES['terrestrial']
terrestrial_shpname = os.path.join(ancillary_path, terr_data['file'])
terr_data_src = driver.Open(terrestrial_shpname, 0)
terrlyr = terr_data_src.GetLayer()
terrflddata = terr_data['fields']
terrindex = rtree.index.Index(interleaved=False)
terrfeats = {}
######### LOOP ##########
terrfeat = terrlyr.GetNextFeature()
while terrfeat is not None:
    terrfid = terrfeat.GetFID()
    terrgeom = terrfeat.GetGeometryRef()
    self._log.info('  {} Geometry {} count {}'.format(
        terrfid, terrgeom.GetGeometryName(), terrgeom.GetGeometryCount()))
    xmin, xmax, ymin, ymax = terrgeom.GetEnvelope()
    terrindex.insert(terrfid, (xmin, xmax, ymin, ymax))
    terrfeats[terrfid] = {'feature': terrfeat, 
                          'geom': terrgeom,
                          'type': terrgeom.GetGeometryName(), 
                          'count': terrgeom.GetGeometryCount()}
    for reffld, bisonfld in terrflddata:
        terrfeats[terrfid][bisonfld] = terrfeat.GetFieldAsString(reffld)
    terrfeat = terrlyr.GetNextFeature()
######### ENDLOOP ##########

# terrindex, terrfeats, terr_bison_fldnames = \
#     self._create_spatial_index(terr_data['fields'], terrlyr)

marine_data = ANCILLARY_FILES['marine']
marine_shpname = os.path.join(ancillary_path, marine_data['file'])
mar_data_src = driver.Open(marine_shpname, 0)
marlyr = mar_data_src.GetLayer()
marflddata = marine_data['fields']
marindex = rtree.index.Index(interleaved=False)
marfeats = {}
######### LOOP ##########
marfeat = marlyr.GetNextFeature()
while marfeat is not None:
    marfid = marfeat.GetFID()
    margeom = marfeat.GetGeometryRef()
    self._log.info('  {} Geometry {} count {}'.format(
        marfid, margeom.GetGeometryName(), margeom.GetGeometryCount()))
    xmin, xmax, ymin, ymax = margeom.GetEnvelope()
    marindex.insert(marfid, (xmin, xmax, ymin, ymax))
    marfeats[marfid] = {'feature': marfeat, 
                        'geom': margeom,
                        'type': margeom.GetGeometryName(), 
                        'count': margeom.GetGeometryCount()}
    for reffld, bisonfld in marflddata:
        marfeats[marfid][bisonfld] = marfeat.GetFieldAsString(reffld)
    marfeat = marlyr.GetNextFeature()
######### ENDLOOP ##########


marine_data = ANCILLARY_FILES['marine']
mar2_shpname = os.path.join(ancillary_path, 
    'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
mar2_data_src = driver.Open(mar2_shpname, 0)
mar2lyr = mar2_data_src.GetLayer()
mar2index = rtree.index.Index(interleaved=False)
mar2feats = {}
######### LOOP ##########
mar2feat = mar2lyr.GetNextFeature()
while mar2feat is not None:
    mar2fid = mar2feat.GetFID()
    mar2geom = mar2feat.GetGeometryRef()
    self._log.info('  {} Geometry {} count {}'.format(
        mar2fid, mar2geom.GetGeometryName(), mar2geom.GetGeometryCount()))
    xmin, xmax, ymin, ymax = mar2geom.GetEnvelope()
    mar2index.insert(mar2fid, (xmin, xmax, ymin, ymax))
    mar2feats[mar2fid] = {'feature': mar2feat, 
                         'geom': mar2geom,
                          'type': mar2geom.GetGeometryName(), 
                          'count': mar2geom.GetGeometryCount()}
    for reffld, bisonfld in marflddata:
        mar2feats[mar2fid][bisonfld] = mar2feat.GetFieldAsString(reffld)
    mar2feat = mar2lyr.GetNextFeature()
######### ENDLOOP ##########


multipoly 215, 217, 218, 232, 

poly, 244, 248, 249

sgs = []
msgs = []
g = mar2feats[215]['geom']
if g.GetGeometryName() == 'MULTIPOLYGON':
    print('MULTIPOLY')
    for i in range(g.GetGeometryCount()):
        msg = g.GetGeometryRef(i)
        msgs.append(msg)
        print('{} subgeom, simple {}, count {}'.format(msg.GetGeometryName(), msg.IsSimple(), msg.GetGeometryCount()))
elif g.GetGeometryName() == 'POLYGON':
    print('POLY')
    for i in range(g.GetGeometryCount()):
        sg = g.GetGeometryRef(i)
        sgs.append(sg)
        print('{} subgeom, simple {}, count {}'.format(sg.GetGeometryName(), sg.IsSimple(), sg.GetGeometryCount()))


# testme
for i in range(margeom.GetGeometryCount()):
    mg = margeom.GetGeometryRef(i)

 
# marindex, marfeats, mar_bison_fldnames = \
#     self._create_spatial_index(marine_data['fields'], eezlyr)



"""
