from osgeo import ogr
import os
import rtree
import time

from common.constants import (BISON_DELIMITER, ENCODING, LOGINTERVAL, 
                              PROHIBITED_CHARS, PROHIBITED_SNAME_CHARS, 
                              PROHIBITED_VALS, 
                              BISON_VALUES, BISON_SQUID_FLD, ITIS_KINGDOMS, 
                              ISO_COUNTRY_CODES, 
                              BISON_ORDERED_DATALOAD_FIELD_TYPE)
from common.inputdata import ANCILLARY_FILES
from common.lookup import Lookup, VAL_TYPE
from common.tools import (get_csv_dict_reader, open_csv_files, get_logger)

# .............................................................................
# ...............................................
# if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser(
#                 description=("""Merge US counties and Canada counties 
#                 shapefiles into a single shapefile, and add centroids"""))
#     parser.add_argument('filepath', type=str, 
#                         help="""Full path to shapefiles to merge.""")
#     parser.add_argument('filename', type=str, 
#                         help="""Base filename for merged output shapefile.""")
#     args = parser.parse_args()
# filepath = args.filepath
# outfname = args.filename


filepath = '/tank/data/bison/2019/ancillary'
overwrite = True

logbasename = 'merge_us_can'
cnty_shpname = os.path.join(filepath, 'us_can_county_centroid.shp')
mar_shpname = os.path.join(filepath, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
mar2_shpname = os.path.join(filepath, 'marine_polygons.shp')
driver = ogr.GetDriverByName("ESRI Shapefile")

# cnty_src = driver.Open(cnty_shpname, 0)
# cnty_lyr = cnty_src.GetLayer()
# cnty_def = cnty_lyr.GetLayerDefn()
# idx_fips = cnty_def.GetFieldIndex('B_FIPS')
# idx_cnty = cnty_def.GetFieldIndex('B_COUNTY')
# idx_st = cnty_def.GetFieldIndex('B_STATE')
# idx_wkt = cnty_def.GetFieldIndex('B_CENTROID')

mar_src = driver.Open(mar_shpname, 0)
mar_lyr = mar_src.GetLayer()
mar_def = mar_lyr.GetLayerDefn()
idx_eez = mar_def.GetFieldIndex('EEZ')
idx_mrgid = mar_def.GetFieldIndex('MRGID')
print ('Old Layer has {} features'.format(mar_lyr.GetFeatureCount()))

mar2_src = driver.Open(mar2_shpname, 0)
mar2_lyr = mar2_src.GetLayer()
mar2_def = mar2_lyr.GetLayerDefn()
idx2_eez = mar2_def.GetFieldIndex('EEZ')
idx2_mrgid = mar2_def.GetFieldIndex('MRGID')
print ('New Layer has {} features'.format(mar2_lyr.GetFeatureCount()))
centroids = None
datadict = {}

for i in range(mar_def.GetFieldCount()):
    fieldName =  mar_def.GetFieldDefn(i).GetName()
    print(fieldName)
# ********************************************************
# Orig Marine layer
polys = {}
mpolys = {}

feat = mar_lyr.GetNextFeature()
while feat is not None:  
    geom = feat.GetGeometryRef()
    pcount = geom.GetGeometryCount()
    eez = feat.GetFieldAsString(idx_eez)
    fid = feat.GetFID()
    if geom.GetGeometryType() == ogr.wkbMultiPolygon:
        mpolys[fid] = (eez, feat)
        print('Multipoly {} EEZ {} has {} polygons'.format(fid, eez, pcount))
    elif geom.GetGeometryType() == ogr.wkbPolygon:
        polys[fid] = (eez, feat)
        print('Poly {} EEZ {} has {} points'.format(fid, eez, pcount))
    feat = mar_lyr.GetNextFeature()



# ********************************************************
# New Marine layer
polys2 = {}
mpolys2 = {}

feat2 = mar2_lyr.GetNextFeature()
while feat2 is not None:  
    geom2 = feat2.GetGeometryRef()
    pcount = geom2.GetGeometryCount()
    eez = feat2.GetFieldAsString(idx_eez)
    fid = feat2.GetFID()
    if geom2.GetGeometryType() == ogr.wkbMultiPolygon:
        mpolys2[fid] = (eez, feat2)
        print('Multipoly {} EEZ {} has {} polygons'.format(fid, eez, pcount))
    elif geom2.GetGeometryType() == ogr.wkbPolygon:
        polys2[fid] = (eez, feat2)
        print('Poly {} EEZ {} has {} points'.format(fid, eez, pcount))
    feat2 = mar2_lyr.GetNextFeature()



mar_lyr = None
mar2_lyr = None


# county = poly.GetFieldAsString(idx_cnty)
# state = poly.GetFieldAsString(idx_st)



# eez = poly.GetFieldAsString(idx_fips)
# county = poly.GetFieldAsString(idx_cnty)
# state = poly.GetFieldAsString(idx_st)
# centroid = poly.GetFieldAsString(idx_centroid)
# key = ';'.join([state, county, fips])
# tmp = centroid.lstrip('Point (').rstrip(')')
# lonstr, latstr = tmp.split(' ')
# datadict[key] = (lonstr, latstr)



# for poly in mar_lyr:
#     eez = poly.GetFieldAsString(idx_fips)
#     county = poly.GetFieldAsString(idx_cnty)
#     state = poly.GetFieldAsString(idx_st)
#     centroid = poly.GetFieldAsString(idx_centroid)
#     key = ';'.join([state, county, fips])
#     tmp = centroid.lstrip('Point (').rstrip(')')
#     lonstr, latstr = tmp.split(' ')
#     datadict[key] = (lonstr, latstr)
