"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
import os
import time
from logging import INFO, DEBUG, ERROR
from osgeo import ogr
import rtree

from bison.common.constants import REGION


# .............................................................................
class GeoResolver(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(
            self, full_spatial_fname, spatial_fields, logger=None, is_disjoint=True,
            buffer_vals=()):
        """Construct a geospatial index to intersect with a set of coordinates.

        Args:
            full_spatial_fname (str): full filename for a shapefile to construct index
            spatial_fields (dict): dictionary containing keys that are fieldnames for
                polygon attributes of interest and values that are new fields for
                the intersected points.
            logger (object): logger for saving relevant processing messages
            is_disjoint (bool): True indicates the spatial features are disjoint;
                False indicates contiguous features.
            buffer_vals (list of floats): range of values to use for buffering the
                point if no intersection is found.

        Raises:
            FileNotFoundError: if spatial_fname does not exist on the file system
        """
        # full_spatial_fname = os.path.join(DATA_PATH, spatial_fname)
        if not os.path.exists(full_spatial_fname):
            raise FileNotFoundError(f"{full_spatial_fname}")
        self._log = logger
        self._spatial_filename = full_spatial_fname
        self._spatial_fields = spatial_fields
        self._is_disjoint = is_disjoint
        self._buffer_vals = buffer_vals
        self.spatial_index = None
        self.spatial_feats = None
        self.bison_spatial_fields = None
        self._initialize_geospatial_data()

    # ...............................................
    @property
    def filename(self):
        """Filename of input geospatial data.

        Returns:
            Input filename
        """
        return os.path.basename(self._spatial_filename)

    # ...............................................
    @property
    def fieldmap(self):
        """Return a dictionary mapping original to bison fieldnames.

        Returns:
            Dictionary of keys of original fieldnames and values of bison fieldnames.
        """
        return self._spatial_fields

    # ...............................................
    def _initialize_geospatial_data(self):
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
        for in_fld, bison_fld in self._spatial_fields.items():
            in_idx = lyr_def.GetFieldIndex(in_fld)
            fld_indexes.append((bison_fld, in_idx))
            bison_fldnames.append(bison_fld)

        # If interleaved is False, the coordinates must be in the form
        # [xmin, xmax, ymin, ymax, …, …, kmin, kmax]
        # aka the same order that OGR outputs
        sp_index = rtree.index.Index(interleaved=False)
        sp_feats = {}
        for fid in range(0, lyr.GetFeatureCount()):
            try:
                feat = lyr.GetFeature(fid)
                geom = feat.geometry()
                # OGR returns xmin, xmax, ymin, ymax
                xmin, xmax, ymin, ymax = geom.GetEnvelope()
                # Rtree takes xmin, xmax, ymin, ymax IFF interleaved = False
                sp_index.insert(fid, (xmin, xmax, ymin, ymax))
                sp_feats[fid] = {'feature': feat, 'geom': geom}
                for name, idx in fld_indexes:
                    sp_feats[fid][name] = feat.GetFieldAsString(idx)
            except Exception as e:
                self.logit(
                    f"Warning, unable to add FID {fid} for {self.filename}: {e}")
        return sp_index, sp_feats, bison_fldnames

    # ...............................................
    def _get_best_features(self, geom, intersect_fids):
        best_fids = []
        if not intersect_fids:
            raise Exception("Give me something to work with here!")
        # Check any intersecting envelopes for actual intersecting geometry
        feature_distances = {}
        for fid in intersect_fids:
            feature_geom = self.spatial_feats[fid]['geom']
            if geom.Intersects(feature_geom):
                feature_distances[fid] = geom.Distance(feature_geom)
        # Now choose the first, closest
        if feature_distances:
            shortest_distance = min(feature_distances.values())
            for fid, dist in feature_distances.items():
                if dist == shortest_distance:
                    best_fids.append(fid)
        return best_fids

    # ...............................................
    def _intersect_with_spatial_index(self, pt):
        """Find FIDs of features in spatial index whose envelopes intersect with point.

        Args:
            pt (ogr point): point to find enclosing polygon from the spatial index

        Returns:
            intersect_fids (list of integers): feature IDs of geometries whose
                envelopes intersect with the original or buffered  point.

        Note:
            Points should always intersect with a feature in contiguous spatial
                datasets, so check for nearest polygon, or buffer the point, to find a
                feature.  Only some points will intersect with a feature in disjoint
                spatial datasets, so do not buffer or check for nearest.
        """
        # First, find intersecting features from spatial index
        intersect_fids = list(self.spatial_index.intersection(pt.GetEnvelope()))
        # For contiguous spatial datasets, keep trying
        if not intersect_fids and self._is_disjoint is False:
            # Second, find nearest feature in spatial index
            intersect_fids = list(self.spatial_index.nearest(pt.GetEnvelope()))
            if not intersect_fids:
                self.logit("Buffer to intersect with contiguous spatial index")
                # Third, try increasingly large buffers to find intersection
                for buffer in self._buffer_vals:
                    geom = pt.Buffer(buffer)
                    # Intersect buffered point with spatial index
                    intersect_fids = list(
                        self.spatial_index.intersection(geom.GetEnvelope()))
                    if intersect_fids:
                        break
                if intersect_fids:
                    self.logit(
                        f"Intersected {len(intersect_fids)} features in spatial "
                        f"index with {buffer} dd buffer.",
                        refname=self.__class__.__name__)
        return intersect_fids

    # ...............................................
    def _intersect_with_polygons(self, pt, intersect_fids):
        """Intersect a point with polygons to find which feature(s) intersect.

        Args:
            pt (ogr point): point to find close polygons from the spatial index
            intersect_fids (list of integers): feature IDs of geometries whose
                envelopes intersect with the point.

        Returns:
            fid (int): feature ID of geometry that intersects (or is closest to) point.
        """
        # Get feature ID from intersecting feature
        fids = self._get_best_features(pt, intersect_fids)
        if not fids:
            geom = pt
            # If provided, try increasingly large buffer
            # (0.1 dd ~= 11.1 km, 1 dd ~= 111 km)
            for buffer in self._buffer_vals:
                geom = geom.Buffer(buffer)
                # If failed to intersect contiguous data, buffer and try again
                fids = self._get_best_features(geom, intersect_fids)
                if fids:
                    break
        if not fids and self._is_disjoint is False:
            self.logit(
                f"Intersected 0 of {len(intersect_fids)} features returned from "
                f"spatial index for contiguous data {self.filename}, with buffers "
                f"{self._buffer_vals}", log_level=DEBUG)
        return fids

    # ...............................................
    def _find_intersecting_feature_values(self, pt):
        """Intersect a point with spatial index, then intersect with returned features.

        Args:
            pt (ogr point): point to find intersecting or closest polygon from the
                spatial data

        Returns:
            fldval_list (list of dict): list of dictionaries of fieldnames and values
                from the intersecting polygons.

        Raises:
            GeoException: on failure to find intersecting/close features from the
                spatial index.
            GeoException: on failure to find intersecting/close features from the
                features identified by the spatial index.
        """
        fldval_list = []
        # Intersect with spatial index to get ID (fid) of intersecting features
        intersect_fids = self._intersect_with_spatial_index(pt)
        if not intersect_fids:
            if self._is_disjoint is False:
                raise GeoException("Failed to find polygon in contiguous spatial index")
        else:
            # Pull attributes of interest from intersecting feature
            fids = self._intersect_with_polygons(pt, intersect_fids)
            if not fids:
                if self._is_disjoint is False:
                    raise GeoException(
                        f"Failed to intersect with any of {len(intersect_fids)} "
                        "spatial index intersections")

            # Retrieve values from intersecting polygons
            for fid in fids:
                fldvals = {}
                for fn in self.bison_spatial_fields:
                    fldvals[fn] = self.spatial_feats[fid][fn]
                fldval_list.append(fldvals)

        return fldval_list

    # ...............................................
    def find_enclosing_polygon_attributes(self, lon, lat):
        """Return attributes of polygon enclosing these coordinates.

        Args:
            lon (str or double): longitude value
            lat (str or double): latitude value

        Returns:
            fldvals (list of dict): list of fieldnames and value dictionaries
            ogr_seconds (double): time elapsed for intersect

        Raises:
            ValueError: on non-numeric coordinate
        """
        fldval_list = []
        try:
            lon = float(lon)
            lat = float(lat)
        except ValueError:
            raise ValueError(f"Longitude {lon} or latitude {lat} is not a number")

        start = time.time()
        # Construct point
        geom = ogr.Geometry(ogr.wkbPoint)
        geom.AddPoint(lon, lat)
        # Intersect with spatial index to get ID (fid) of intersecting features
        try:
            fldval_list = self._find_intersecting_feature_values(geom)
        except ValueError:
            raise
        except GeoException as e:
            self.logit(
                f"No polygon found: {e}", refname=self.__class__.__name__,
                log_level=ERROR)

        # Elapsed time
        ogr_seconds = time.time()-start
        if ogr_seconds > 1:
            self.logit(
                f"Intersect geom OGR time {ogr_seconds}",
                refname=self.__class__.__name__, log_level=DEBUG)

        return fldval_list

    # ...............................................
    def logit(self, msg, refname=None, log_level=INFO):
        """Method to log a message to a logger/file/stream or print to console.

        Args:
            msg: message to print.
            refname: calling function name.
            log_level: error level, INFO, DEBUG, WARNING, ERROR
        """
        if self._log is not None:
            self._log.log(msg, refname=refname, log_level=log_level)
        else:
            print(msg)


# .............................................................................
def get_geo_resolvers(geo_input_path, regions, logger=None):
    """Get geospatial indexes for regions in the area of interest.

    Args:
        geo_input_path (str): input path for geospatial files to intersect points
        regions (sequence of common.constants.REGION): list of REGION members for
            to retrieve spatial index resolvers for.
        logger (object): logger for saving relevant processing messages

    Returns:
        geo_fulls (list): spatial indexes that each cover a region.
        pad_subsets (dict): spatial indexes that together comprise the PAD region.
    """
    geo_fulls = []
    pad_subsets = {}
    for region in regions:
        if region in REGION.full_region():
            fn = os.path.join(geo_input_path, region["file"])
            geo_fulls.append(GeoResolver(
                fn, region["map"], logger=logger, is_disjoint=region["is_disjoint"],
                buffer_vals=region["buffer"]))

        elif region == REGION.PAD:
            state_file = REGION.get_state_files_from_pattern(geo_input_path)
            for subset, full_fn in state_file.items():
                pad_subsets[subset] = GeoResolver(
                    full_fn, REGION.PAD["map"], logger=logger,
                    is_disjoint=REGION.PAD["is_disjoint"],
                    buffer_vals=REGION.PAD["buffer"])

    return geo_fulls, pad_subsets


# .............................................................................
class GeoException(Exception):
    """Object for returning geospatial index errors."""
    def __init__(self, message=""):
        """Constructor.

        Args:
            message: optional message attached to Exception.
        """
        Exception(message)


# .............................................................................
__all__ = [
    "GeoException",
    "GeoResolver",
    "get_geo_resolvers",
]
