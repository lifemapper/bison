import os
from osgeo import ogr

from bison.common.constants import DATA_PATH, US_COUNTY

# .............................................................................
class GeoResolver(object):
    """
    Object for processing a CSV file with 47 ordered BISON fields (and optional
    gbifID field for GBIF provided data) filling ITIS, coordinates, terrestrial
    or marine boundaries, and establishment_means.
    """

    # ...............................................
    def __init__(self, spatial_fname, spatial_fields, log):
        """
        @summary: Constructor
        """
        full_spatial_fname = os.path.join(DATA_PATH, spatial_fname)
        if not os.path.exists(full_spatial_fname):
            raise FileNotFoundError
        self._log = log
        self._spatial_filename = full_spatial_fname
        self._spatial_fields = spatial_fields
        self.spatial_index = None
        self.spatial_feats = None
        self.bison_spatial_fields = None

    # ...............................................
    def initialize_geospatial_data(self):
        driver = ogr.GetDriverByName("ESRI Shapefile")

        bnd_src = driver.Open(self._spatial_filename, 0)
        bnd_lyr = bnd_src.GetLayer()
        (self.spatial_index, self.spatial_feats, self.bison_spatial_fields) = self._create_spatial_index(
             self._spatial_fields, bnd_lyr)


    # ...............................................
    def _create_spatial_index(self, flddata, lyr):
        lyr_def = lyr.GetLayerDefn()

        fld_indexes = []
        bison_fldnames = []
        for bnd_fld, bison_fld in flddata.items():
            bnd_idx = lyr_def.GetFieldIndex(bnd_fld)
            fld_indexes.append((bison_fld, bnd_idx))
            bison_fldnames.append(bison_fld)

        sp_index = rtree.index.Index(interleaved=False)
        sp_feats = {}
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            geom = feat.geometry()
            # OGR returns xmin, xmax, ymin, ymax
            xmin, xmax, ymin, ymax = geom.GetEnvelope()
            # Rtree takes xmin, xmax, ymin, ymax IFF interleaved = False
            sp_index.insert(fid, (xmin, xmax, ymin, ymax))
            sp_feats[fid] = {'feature': feat, 'geom': geom}
            for name, idx in fld_indexes:
                sp_feats[fid][name] = feat.GetFieldAsString(idx)
        return sp_index, sp_feats, bison_fldnames

    # ...............................................
    def update_point_in_polygons(self, geo_data):
        """Process a CSV file to intersect coordinates with state and county boundaries"""
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

