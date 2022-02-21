import os
from osgeo import ogr

from bison.common.constants import US_COUNTY

# .............................................................................
class GeoResolver(object):
    """
    Object for processing a CSV file with 47 ordered BISON fields (and optional
    gbifID field for GBIF provided data) filling ITIS, coordinates, terrestrial
    or marine boundaries, and establishment_means.
    """

    # ...............................................
    def __init__(self, state_county_shpfile, log):
        """
        @summary: Constructor
        """
        self._log = log
        if os.path.exists(state_county_shpfile):
            self._us_filename = state_county_shpfile
        self.us_index = None
        self.us_feats = None
        self.us_fldnames = None

    # ...............................................
    def initialize_geospatial_data(self):
        driver = ogr.GetDriverByName("ESRI Shapefile")

        if None in (self.us_index, self.us_feats, self.us_fldnames):
            terrestrial_shpname = os.path.join(ancillary_path, terr_data['file'])
            terr_data_src = driver.Open(terrestrial_shpname, 0)
            terrlyr = terr_data_src.GetLayer()
            (self.terrindex,
             self.terrfeats,
             self.terr_bison_fldnames) = self._create_spatial_index(
                 terr_data['fields'], terrlyr)

        if None in (self.marindex, self.marfeats, self.mar_bison_fldnames):
            marine_shpname = os.path.join(ancillary_path, marine_data['file'])
            eez_data_src = driver.Open(marine_shpname, 0)
            eezlyr = eez_data_src.GetLayer()
            (self.marindex,
             self.marfeats,
             self.mar_bison_fldnames) = self._create_spatial_index(
                 marine_data['fields'], eezlyr)



    # ...............................................
    def update_point_in_polygons(self, geo_data):
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
        if os.path.exists(outfname):
            delete_shapefile(outfname)
        if self.is_open():
            self.close()

        self.initialize_geospatial_data(terr_data, marine_data, ancillary_path)

        matches = {0: 0, 1: 0, 2: 0}
        recno = 0
        painful_count = 0
        try:
            start_time = time.time()
            loop_time = start_time
            dict_reader, inf, writer, outf = open_csv_files(
                infname, BISON_DELIMITER, ENCODING, outfname=outfname,
                outfields=self._outfields)
            for rec in dict_reader:
                recno += 1
                if from_gbif:
                    squid = rec['id']
                else:
                    squid = rec[BISON_SQUID_FLD]
                lon, lat = self._get_coords(rec)
                # Use coordinates to calc
                if lon is not None:
                    # Compute geo: coordinates and polygons
                    match_count, msgs = self._fill_geofields(rec, lon, lat)

                    if match_count == 0:
                        matches[0] += 1
                    elif match_count == 1:
                        matches[1] += 1
                    else:
                        matches[2] += 1

                    if msgs:
                        painful_count += 1
                        for msg in msgs:
                            self._log.info('Rec {}; {}'.format(recno, msg))

                # Write updated record
                row = self._makerow(rec)
                writer.writerow(row)
                # Log progress occasionally, this process is very time-consuming
                # so show progress at shorter intervals to ensure it is moving
                if (recno % LOGINTERVAL/10) == 0:
                    now = time.time()
                    self._log.info('*** Record number {}, painful {}, time {} ***'.format(
                        recno, painful_count, now - loop_time))
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

