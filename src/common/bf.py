import ogr
import os
import rtree
import time

from common.constants import (BISON_DELIMITER, ENCODING,BISON_SQUID_FLD)
from common.tools import (getCSVDictReader)
from common.bisonfill import BisonFiller
from common.tools import (getCSVDictReader, getCSVDictWriter, getLogger)

gbif_interp_file='/tank/data/bison/2019/AS/occurrence.txt'
gbif_interp_file='/tank/data/bison/2019/Terr/occurrence_lines_1-10000001.txt'

tmpdir = 'tmp'
outdir = 'out'
infname = '/tank/data/bison/2019/AS/tmp/step3_itis_geo_estmeans_occurrence.csv'
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
terrestrial_shpname = os.path.join(ancillary_path, 'US_CA_Counties_Centroids.shp')
estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
marine_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')

# reference files for lookups
dataset_lut_fname = os.path.join(tmppath, 'dataset_lut.csv')
org_lut_fname = os.path.join(tmppath, 'organization_lut.csv')
itis1_lut_fname = os.path.join(tmppath, 'step3_itis_lut.txt')

driver = ogr.GetDriverByName("ESRI Shapefile")
terr_data_src = driver.Open(terrestrial_shpname, 0)
terrlyr = terr_data_src.GetLayer()
terr_def = terrlyr.GetLayerDefn()
idx_fips = terr_def.GetFieldIndex('FIPS')
idx_cnty = terr_def.GetFieldIndex('COUNTY_NAM')
idx_st = terr_def.GetFieldIndex('STATE_NAME')

eez_data_src = driver.Open(marine_shpname, 0)
eezlyr = eez_data_src.GetLayer()
eez_def = eezlyr.GetLayerDefn()
idx_eez = eez_def.GetFieldIndex('EEZ')
idx_mg = eez_def.GetFieldIndex('MRGID')

terrindex = rtree.index.Index(interleaved=False)
terrfeats = {}
for fid in range(0, terrlyr.GetFeatureCount()):
    poly = terrlyr.GetFeature(fid)
    geom = poly.GetGeometryRef()
    xmin, xmax, ymin, ymax = geom.GetEnvelope()
    terrindex.insert(fid, (xmin, xmax, ymin, ymax))
    terrfeats[fid] = {'geom': geom, 
                      'fips': poly.GetFieldAsString(idx_fips), 
                      'county': poly.GetFieldAsString(idx_cnty), 
                      'state': poly.GetFieldAsString(idx_st)}

def get_coords(rec):
    slon = rec['longitude']
    slat = rec['latitude']
    try:
        lon = float(slon)
        lat = float(slat)
    except:
        lon = lat = None
    return lon, lat


recno = 0
points = []
try:
    drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
    for rec in drdr:
        if recno > 10000:
            break
        recno += 1
        squid = rec[BISON_SQUID_FLD]
        lon, lat = get_coords(rec)
        # Use coordinates to calc 
        if lon is not None:
            points.append((lon, lat))
except Exception as e:
    print('Failed reading data from id {}, line {}: {}'
                    .format(squid, recno, e))                    
finally:
    inf.close()

# ...............................................
def get_geofields_wo_rtree(lon, lat,
                    terrlyr, idx_fips, idx_cnty, idx_st, 
                    eezlyr, idx_eez, idx_mg):
    fips = county = state = eez = mrgid = None
    terr_count = marine_count = 0
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.AddPoint(lon, lat)
    terrlyr.SetSpatialFilter(pt)
    eezlyr.SetSpatialFilter(pt)
    for poly in terrlyr:
        if terr_count == 0:
            terr_count += 1
            fips = poly.GetFieldAsString(idx_fips)
            county = poly.GetFieldAsString(idx_cnty)
            state = poly.GetFieldAsString(idx_st)
        else:
            terr_count += 1
            fips = county = state = None
            break
    terrlyr.ResetReading()
    return (fips, county, state, eez, mrgid)
#     # Single terrestrial polygon takes precedence
#     if terr_count == 1:
#         return (fips, county, state, eez, mrgid)
#     else:
#     # Single marine intersect is 2nd choice
#     elif terr_count == 0:
#         for poly in eezlyr:
#             if marine_count == 0:
#                 marine_count += 1
#                 eez = poly.GetFieldAsString(idx_eez)
#                 mrgid = poly.GetFieldAsString(idx_mg)
#             else:
#                 marine_count += 1
#                 eez = mrgid = None
#                 break
#         eezlyr.ResetReading()
#     return (fips, county, state, eez, mrgid)
#
# ...............................................
def get_geofields_with_rtree(lon, lat, terrfeats, terrindex, 
                             marfeats, marindex):
    fips = county = state = eez = mrgid = None
    terr_count = mar_count = 0
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.AddPoint(lon, lat)
    
    for tfid in list(terrindex.intersection((lon, lat))):
        tpoly = terrfeats[tfid]['geom']
        if pt.Within(tpoly):
            if terr_count == 0:
                terr_count += 1
                fips = terrfeats[tfid]['fips']
                county = terrfeats[tfid]['county']
                state = terrfeats[tfid]['state']
            else:
                terr_count += 1
                fips = county = state = None
                break
    return (fips, county, state, eez, mrgid)
#     # Single terrestrial polygon takes precedence
#     if terr_count == 1:
#         return (fips, county, state, eez, mrgid)
#     # Single marine intersect is 2nd choice
#     elif terr_count == 0:
#         for mfid in list(marindex.intersection((lon, lat))):
#             mpoly = marfeats[mfid]['geom']
#             if pt.Within(mpoly):
#                 if mar_count == 0:
#                     mar_count += 1
#                     eez = terrfeats[tfid]['eez']
#                     mrgid = terrfeats[tfid]['mrgid']
#                 else:
#                     mar_count += 1
#                     eez = mrgid = None
#                     break
#     return (fips, county, state, eez, mrgid)
    

# marindex = rtree.index.Index(interleaved=False)
# marfeats = {}
# for fid in range(0, eezlyr.GetFeatureCount()):
#     feat = eezlyr.GetFeature(fid)
#     geom = feat.GetGeometryRef()
#     xmin, xmax, ymin, ymax = geom.GetEnvelope()
#     marindex.insert(fid, (xmin, xmax, ymin, ymax))
#     marfeats[fid] = {'geom': geom, 
#                      'eez': poly.GetFieldAsString(idx_eez), 
#                      'mrgid': poly.GetFieldAsString(idx_mg)}
    
    
    


i = 0
lon, lat = points[i]
pt = ogr.Geometry(ogr.wkbPoint)
pt.AddPoint(lon, lat)

# for tfid in list(terrindex.intersection((lon, lat))):

# ...............................................
# ...............................................
tpoly = terrfeats[tfid]['geom']
if pt.Within(tpoly):
    if terr_count == 0:
        terr_count += 1
        fips = terrfeats[tfid]['fips']
        county = terrfeats[tfid]['county']
        state = terrfeats[tfid]['state']
    else:
        terr_count += 1
        fips = county = state = None
        break
print(fips, county, state, eez, mrgid)

(fips, county, state, eez,  mrgid) = get_geofields_with_rtree(lon, lat, terrfeats, terrindex, 
                                   marfeats, marindex)

print(fips, county, state, eez, mrgid)
i++

# ...............................................
# ...............................................
rtree_start = time.time()
for i in range(len(points)):
    lon, lat = points[i]
    try:
        (fips, county, state, eez, 
         mrgid) = get_geofields_with_rtree(lon, lat, terrfeats, terrindex, 
                                           marfeats, marindex)
    except Exception as e:
        print ('Failed on record {} with {}'.format(i, e))

rtree_stop = time.time()
rtree_elapsed = rtree_stop - rtree_start

ogr_start = time.time()
for i in range(len(points)):
    lon, lat = points[i]
    try:
        (fips, county, state, eez, 
         mrgid) = get_geofields_wo_rtree(lon, lat, terrlyr, idx_fips, 
                                         idx_cnty, idx_st, eezlyr, 
                                         idx_eez, idx_mg)
    except Exception as e:
        print ('Failed on record {} with {}'.format(i, e))

ogr_stop = time.time()
ogr_elapsed = ogr_stop - ogr_start

# ...............................................
# ...............................................

infname = '/tank/data/bison/2019/Terr/tmp/step4_occurrence_lines_1-10000001.csv'
outfname = '/tank/data/bison/2019/Terr/tmp/smoketest_1-2638050.csv'
self = BisonFiller(infname)

drdr, inf = getCSVDictReader(infname, BISON_DELIMITER, ENCODING)
self._files.append(inf)


deleteme = []
for fld in self._bison_ordered_flds:
    if fld not in drdr.fieldnames:
        deleteme.append(fld)

for fld in deleteme:
    self._bison_ordered_flds.remove(fld)

dwtr, outf = getCSVDictWriter(outfname, BISON_DELIMITER, ENCODING, 
                              self._bison_ordered_flds)

dwtr.writeheader()
self._files.append(outf)

recno = 0
for rec in drdr:
    rec.pop('taxonKey')
    dwtr.writerow(rec)
    recno += 1

