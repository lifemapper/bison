"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
import os
import time

import ogr
import rtree


# .............................................................................
class GeoResolver(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(self, full_spatial_fname, spatial_fields, logger):
        """Construct a geospatial index to intersect with a set of coordinates.

        Args:
            full_spatial_fname (str): full filename for a shapefile to construct index
            spatial_fields (dict): dictionary containing keys that are fieldnames for
                polygon attributes of interest and values that are new fields for
                the intersected points.
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: if spatial_fname does not exist on the file system
        """
        # full_spatial_fname = os.path.join(DATA_PATH, spatial_fname)
        if not os.path.exists(full_spatial_fname):
            raise FileNotFoundError
        self._log = logger
        self._spatial_filename = full_spatial_fname
        self._spatial_fields = spatial_fields
        self.spatial_index = None
        self.spatial_feats = None
        self.bison_spatial_fields = None
        self._initialize_geospatial_data()

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
    def _get_first_best_feature(self, geom, intersect_fids):
        best_fid = None
        if not intersect_fids:
            raise Exception("Give me something to work with here!")
        # Check any intersecting envelopes for actual intersecting geometry
        success_fids = []
        feature_distances = {}
        for fid in intersect_fids:
            feature_geom = self.spatial_feats[fid]['geom']
            feature_distances[fid] = geom.Distance(feature_geom)
            if geom.Intersects(feature_geom):
                success_fids.append(fid)
        # if one intersected, yay!
        if len(success_fids) == 1:
            best_fid = success_fids[0]
        # one or more from spatial index, find closest!
        else:
            for fid, dist in feature_distances.items():
                if best_fid is None:
                    best_fid = fid
                    best_dist = dist
                elif dist < best_dist:
                    best_fid = fid
                    best_dist = dist
            # self._log.info(
            #   f"Found feature closest to geom from {len(intersect_fids)} features")
        return best_fid

    # ...............................................
    def _intersect_with_spatial_index(self, pt, buffer_vals):
        """Find FIDs of features in spatial index whose envelopes intersect with point.

        Args:
            pt (ogr point): point to find close polygons from the spatial index
            buffer_vals (list of floats): range of values to use for buffering the
                point if no intersection is found.

        Returns:
            intersect_fids (list of integers): feature IDs of geometries whose
                envelopes intersect with the original or buffered  point.
        """
        # First, find intersecting features from spatial index
        intersect_fids = list(self.spatial_index.intersection(pt.GetEnvelope()))
        # Second, try nearest feature in spatial index
        if not intersect_fids:
            intersect_fids = list(self.spatial_index.nearest(pt.GetEnvelope()))
            # Third, try buffer point and find intersection
            if not intersect_fids:
                self._log.info("Must buffer point to intersect with spatial index!")
                if buffer_vals is not None:
                    geom = pt
                    for buffer in buffer_vals:
                        geom = geom.Buffer(buffer)
                        # Intersect buffered point with spatial index
                        intersect_fids = list(
                            self.spatial_index.intersection(geom.GetEnvelope()))
                        if intersect_fids:
                            break
                    if not intersect_fids:
                        self._log.error(f"Failed to intersect point buffered to "
                                        f"{buffer} dd with spatial index")
                    else:
                        self._log.info(
                            f"Intersected point buffered {buffer} dd returned "
                            f"{len(intersect_fids)} features")
        return intersect_fids

    # ...............................................
    def _intersect_with_polygons(self, pt, intersect_fids, buffer_vals):
        """Intersect a point with polygons to find which feature contains/is closest to.

        Args:
            pt (ogr point): point to find close polygons from the spatial index
            intersect_fids (list of integers): feature IDs of geometries whose
                envelopes intersect with the point.
            buffer_vals (list of floats): range of values to use for buffering the
                point if no intersection is found.

        Returns:
            fid (int): feature ID of geometry that intersects (or is closest to) point.
        """
        # Get feature ID from intersecting feature
        fid = self._get_first_best_feature(pt, intersect_fids)
        if fid is None:
            # Buffer may be unnecessary
            self._log.info("Must buffer point to find best feature!")
            if buffer_vals is not None:
                geom = pt
                # Try increasingly large buffer, (0.1 dd ~= 11.1 km, 1 dd ~= 111 km)
                for buffer in buffer_vals:
                    geom = geom.Buffer(buffer)
                    fid = self._get_first_best_feature(geom, intersect_fids)
                    if fid is not None:
                        break
                if fid is not None:
                    self._log.info(
                        f"Found best feature {fid} using buffer {buffer} dd")
                else:
                    self._log.info(
                        f"Failed to intersect point using buffer {buffer} dd")
        return fid

    # ...............................................
    def _intersect_and_buffer(self, pt, buffer_vals):
        """Intersect a point with spatial index, then intersect with returned features.

        Args:
            pt (ogr point): point to find intersecting or closest polygon from the
                spatial data
            buffer_vals (list of floats): range of values to use for buffering the
                point if no intersection is found.

        Returns:
            fldvals (dict): dictionary of fieldnames and values from the intersecting
                polygon.

        Raises:
            GeoException: on failure to find intersecting/close features from the
                spatial index.
            GeoException: on failure to find intersecting/close features from the
                features identified by the spatial index.
        """
        fldvals = {}
        # Intersect with spatial index to get ID (fid) of intersecting features
        intersect_fids = self._intersect_with_spatial_index(pt, buffer_vals)
        if not intersect_fids:
            raise GeoException(
                "Failed to intersect or find nearest polygon with spatial index")

        # Pull attributes of interest from intersecting feature
        fid = self._intersect_with_polygons(pt, intersect_fids, buffer_vals)
        if fid is None:
            raise GeoException(
                f"Failed to intersect point with any of {len(intersect_fids)} features")

        # Retrieve values from intersecting polygon
        for fn in self.bison_spatial_fields:
            fldvals[fn] = self.spatial_feats[fid][fn]

        return fldvals

    # ...............................................
    def find_enclosing_polygon(self, lon, lat, buffer_vals=None):
        """Return attributes of polygon enclosing these coordinates.

        Args:
            lon (str or double): longitude value
            lat (str or double): latitude value
            buffer_vals (list of floats): optional list of values for buffering point
                to find intersecting or closest polygon

        Returns:
            fldvals (dict): of fieldnames and values
            ogr_seconds (double): time elapsed for intersect

        Raises:
            ValueError: on non-numeric coordinate
        """
        try:
            lon = float(lon)
            lat = float(lat)
        except ValueError:
            raise ValueError(f"Longitude {lon} or latitude {lat} is not a number")

        start = time.time()
        # Construct point
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(lon, lat)
        # Intersect with spatial index to get ID (fid) of intersecting features,
        # buffer if necessary
        try:
            fldvals = self._intersect_and_buffer(pt, buffer_vals)
        except GeoException as e:
            self._log.error(f"No polygon found: {e}")
            fldvals = {}
            for fn in self.bison_spatial_fields:
                fldvals[fn] = None

        # Elapsed time
        ogr_seconds = time.time()-start

        return fldvals, ogr_seconds


# .............................................................................
class GeoException(Exception):
    """Object for returning geospatial index errors."""
    def __init__(self, message=""):
        """Constructor.

        Args:
            message: optional message attached to Exception.
        """
        Exception(message)
