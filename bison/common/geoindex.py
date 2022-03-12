"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
import os
import ogr
import rtree
import time

from bison.common.constants import DATA_PATH


# .............................................................................
class GeoResolver(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(self, spatial_fname, spatial_fields, logger):
        """Construct a geospatial index to intersect with a set of coordinates.

        Args:
            spatial_fname (str): filename for a shapefile to construct index from
            spatial_fields (dict): dictionary containing keys that are fieldnames for
                polygon attributes of interest and values that are new fields for
                the intersected points.
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: if spatial_fname does not exist on the file system
        """
        full_spatial_fname = os.path.join(DATA_PATH, spatial_fname)
        if not os.path.exists(full_spatial_fname):
            raise FileNotFoundError
        self._log = logger
        self._spatial_filename = full_spatial_fname
        self._spatial_fields = spatial_fields
        self.spatial_index = None
        self.spatial_feats = None
        self.bison_spatial_fields = None

    # ...............................................
    def initialize_geospatial_data(self):
        """Create a spatial index for the features in self._spatial_filename."""
        driver = ogr.GetDriverByName("ESRI Shapefile")

        bnd_src = driver.Open(self._spatial_filename, 0)
        bnd_lyr = bnd_src.GetLayer()
        (self.spatial_index,
         self.spatial_feats,
         self.bison_spatial_fields
         ) = self._create_spatial_index(bnd_lyr)

    # ...............................................
    def _create_spatial_index(self, lyr):
        lyr_def = lyr.GetLayerDefn()
        fld_indexes = []
        bison_fldnames = []
        for bnd_fld, bison_fld in self._spatial_fields.items():
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
    def _get_values_from_polygons(self, pt, intersect_fids):
        # Initialize dictionary
        fldvals = {}
        for fn in self.bison_spatial_fields:
            fldvals[fn] = None
        intersects = False
        # Check any intersecting envelopes for actual intersecting geometry
        for fid in intersect_fids:
            geom = self.spatial_feats[fid]['geom']
            if pt.Intersects(geom):
                intersects = True
                # Retrieve values from intersecting polygon
                for fn in self.bison_spatial_fields:
                    fldvals[fn] = self.spatial_feats[fid][fn]
                # Stop looking after finding intersection
                break
        return intersects, fldvals

    # ...............................................
    def find_enclosing_polygon(self, lon, lat):
        """Return attributes of polygon enclosing these coordinates.

        Args:
            lon (str or double): longitude value
            lat (str or double): latitude value

        Returns:
            fldvals (dict): of fieldnames and values
            ogr_seconds (double): time elapsed for intersect

        Raises:
            ValueError: on non-numeric coordinate
            GeoException: on failure to intersect point with spatial index
            GeoException: on no intersecting geometry returned by spatial index contains point
        """
        ogr_seconds = 0
        # Initialize fields to pull values from intersection
        fldvals = {}
        try:
            lon = float(lon)
            lat = float(lat)
        except ValueError:
            raise ValueError(f"Longitude {lon} or latitude {lat} is not a number")
        else:
            start = time.time()
            # Construct point
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.AddPoint(lon, lat)
            # Intersect with spatial index to get ID (fid) of intersecting features
            intersect_fids = list(self.spatial_index.intersection((lon, lat)))

            # Pull attributes of interest from intersecting feature
            intersects, fldvals = self._get_values_from_polygons(pt, intersect_fids)

            if intersects is False:
                # If point does not intersect spatial index (envelopes of features)
                if not intersect_fids:
                    raise GeoException(f"Failed to intersect point {lon} {lat} with spatial index")
                else:
                    # If point does not intersect geometries, try buffer (0.1 dd ~= 11.1 km) to get close enough
                    ptbuf = pt.Buffer(0.1)
                    intersects, fldvals = self._get_values_from_polygons(ptbuf, intersect_fids)
                    if intersects is False:
                        raise GeoException(f"Failed to intersect buffered point {lon} {lat} with spatial index")

            # Elapsed time
            ogr_seconds = time.time()-start

        return fldvals, ogr_seconds


# .............................................................................
class GeoException(Exception):
    """Object for returning geospatial index errors."""
    def __init__(self, message=""):
        """Constructor.

        Args:
            message: optional message to be displayed when raised.
        """
        Exception(message)
