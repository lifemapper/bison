import ogr
import os
import rtree
import time

from bison.common import (BISON_DELIMITER, ENCODING,
                          ANCILLARY_DIR, ANCILLARY_FILES, )
from bison.common import BisonFiller
from bison.common import (get_csv_dict_reader, get_csv_dict_writer)

testsize = 10000
gbif_interp_file='/tank/data/bison/2019/Terr/occurrence_lines_5000-10000.txt'
tmpdir = 'tmp'
outdir = 'out'


# ...............................................
def rewrite_records(infname, outfname):
    self = BisonFiller(infname)

    drdr, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
    self._files.append(inf)

    deleteme = []
    for fld in self._bison_ordered_flds:
        if fld not in drdr.fieldnames:
            deleteme.append(fld)

    for fld in deleteme:
        self._bison_ordered_flds.remove(fld)

    dwtr, outf = get_csv_dict_writer(outfname, BISON_DELIMITER, ENCODING,
                                  self._bison_ordered_flds)

    dwtr.writeheader()
    self._files.append(outf)

    recno = 0
    for rec in drdr:
        rec.pop('taxonKey')
        dwtr.writerow(rec)
        recno += 1
        print(recno)

# ...............................................
def get_coords(rec):
    slon = rec['longitude']
    slat = rec['latitude']
    try:
        lon = float(slon)
        lat = float(slat)
    except:
        lon = lat = None
    return lon, lat


# ...............................................
def read_some_points(infname, count):
    recno = 0
    points = []
    try:
        drdr, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
        for rec in drdr:
            if recno > count:
                break
            recno += 1
            lon, lat = get_coords(rec)
            if lon is not None:
                points.append((lon, lat))
    except Exception as e:
        print('Failed reading data from record {}: {}'.format(recno, e))
    finally:
        inf.close()
    return points

# ...............................................
def get_geofields_wo_rtree(lon, lat, terrlyr, idx_fips, idx_cnty, idx_st):
    fips = county = state = None
    terr_count = 0
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
    return (fips, county, state)

# ...............................................
def get_geofields_with_rtree(lon, lat, terrindex, terrfeats):
    fips = county = state = None
    terr_count = 0
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.AddPoint(lon, lat)

    for tfid in list(terrindex.intersection((lon, lat))):
        fips = county = state = eez = mrgid = None
#         feat = terrfeats[tfid]['feature']
#         geom = feat.GetGeometryRef()
        geom = terrfeats[tfid]['geom']
        if pt.Within(geom):
            if terr_count == 0:
                terr_count += 1
                fips = terrfeats[tfid]['fips']
                county = terrfeats[tfid]['county']
                state = terrfeats[tfid]['state']
            else:
                terr_count += 1
                fips = county = state = None
                break
    return (fips, county, state)
# ...............................................
#
#
# ...............................................
def create_marine_index(eezlyr, idx_eez, idx_mg):
    marindex = rtree.index.Index(interleaved=False)
    marfeats = {}
    for fid in range(0, eezlyr.GetFeatureCount()):
        feat = eezlyr.GetFeature(fid)
        geom = feat.GetGeometryRef()
        xmin, xmax, ymin, ymax = geom.GetEnvelope()
        marindex.insert(fid, (xmin, xmax, ymin, ymax))
        marfeats[fid] = {'feature': feat,
                         'geom': geom,
                         'eez': feat.GetFieldAsString(idx_eez),
                         'mrgid': feat.GetFieldAsString(idx_mg)}
    return marindex, marfeats

# ...............................................
def create_terr_index(terrlyr, idx_fips, idx_cnty, idx_st):
    terrindex = rtree.index.Index(interleaved=False)
    terrfeats = {}
    for fid in range(0, terrlyr.GetFeatureCount()):
        feat = terrlyr.GetFeature(fid)
        geom = feat.GetGeometryRef()
        xmin, xmax, ymin, ymax = geom.GetEnvelope()
        terrindex.insert(fid, (xmin, xmax, ymin, ymax))
        terrfeats[fid] = {'feature': feat,
                          'geom': geom,
                          'fips': feat.GetFieldAsString(idx_fips),
                          'county': feat.GetFieldAsString(idx_cnty),
                          'state': feat.GetFieldAsString(idx_st)}
    return terrindex, terrfeats

# ...............................................
# Main...........................................
# ...............................................

# infname = '/tank/data/bison/2019/Terr/tmp/step4_occurrence_lines_1-10000001.csv'
# outfname = '/tank/data/bison/2019/Terr/tmp/smoketest_1-2638050.csv'
# rewrite_records(infname, outfname)
#
# driver = ogr.GetDriverByName("ESRI Shapefile")
# terr_data_src = driver.Open(terrestrial_shpname, 0)
# terrlyr = terr_data_src.GetLayer()
# terr_def = terrlyr.GetLayerDefn()
# idx_fips = terr_def.GetFieldIndex('FIPS')
# idx_cnty = terr_def.GetFieldIndex('COUNTY_NAM')
# idx_st = terr_def.GetFieldIndex('STATE_NAME')
#
# eez_data_src = driver.Open(marine_shpname, 0)
# eezlyr = eez_data_src.GetLayer()
# eez_def = eezlyr.GetLayerDefn()
# idx_eez = eez_def.GetFieldIndex('EEZ')
# idx_mg = eez_def.GetFieldIndex('MRGID')

# # ...............................................
# def rewrite_records(infname, outfname):
#     self = BisonFiller(infname)
#
#     drdr, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
#     self._files.append(inf)
#
#     deleteme = []
#     for fld in self._bison_ordered_flds:
#         if fld not in drdr.fieldnames:
#             deleteme.append(fld)
#
#     for fld in deleteme:
#         self._bison_ordered_flds.remove(fld)
#
#     dwtr, outf = get_csv_dict_writer(outfname, BISON_DELIMITER, ENCODING,
#                                   self._bison_ordered_flds)
#
#     dwtr.writeheader()
#     self._files.append(outf)
#
#     recno = 0
#     for rec in drdr:
#         rec.pop('taxonKey')
#         dwtr.writerow(rec)
#         recno += 1
#         print(recno)
#
# # ...............................................
# def get_coords(rec):
#     slon = rec['longitude']
#     slat = rec['latitude']
#     try:
#         lon = float(slon)
#         lat = float(slat)
#     except:
#         lon = lat = None
#     return lon, lat
#
#
# # ...............................................
# def read_some_points(infname, count):
#     recno = 0
#     points = []
#     try:
#         drdr, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
#         for rec in drdr:
#             if recno > count:
#                 break
#             recno += 1
#             lon, lat = get_coords(rec)
#             if lon is not None:
#                 points.append((lon, lat))
#     except Exception as e:
#         print('Failed reading data from record {}: {}'.format(recno, e))
#     finally:
#         inf.close()
#     return points
#
# # ...............................................
# def get_geofields_wo_rtree(lon, lat, terrlyr, idx_fips, idx_cnty, idx_st):
#     fips = county = state = None
#     terr_count = 0
#     pt = ogr.Geometry(ogr.wkbPoint)
#     pt.AddPoint(lon, lat)
#     terrlyr.SetSpatialFilter(pt)
#     eezlyr.SetSpatialFilter(pt)
#     for poly in terrlyr:
#         if terr_count == 0:
#             terr_count += 1
#             fips = poly.GetFieldAsString(idx_fips)
#             county = poly.GetFieldAsString(idx_cnty)
#             state = poly.GetFieldAsString(idx_st)
#         else:
#             terr_count += 1
#             fips = county = state = None
#             break
#     terrlyr.ResetReading()
#     return (fips, county, state)
#
# # ...............................................
# def get_geofields_with_rtree(lon, lat, terrindex, terrfeats):
#     fips = county = state = None
#     terr_count = 0
#     pt = ogr.Geometry(ogr.wkbPoint)
#     pt.AddPoint(lon, lat)
#
#     for tfid in list(terrindex.intersection((lon, lat))):
#         fips = county = state = eez = mrgid = None
# #         feat = terrfeats[tfid]['feature']
# #         geom = feat.GetGeometryRef()
#         geom = terrfeats[tfid]['geom']
#         if pt.Within(geom):
#             if terr_count == 0:
#                 terr_count += 1
#                 fips = terrfeats[tfid]['fips']
#                 county = terrfeats[tfid]['county']
#                 state = terrfeats[tfid]['state']
#             else:
#                 terr_count += 1
#                 fips = county = state = None
#                 break
#     return (fips, county, state)
# # ...............................................
# #
# #
# # ...............................................
# def create_marine_index(eezlyr, idx_eez, idx_mg):
#     marindex = rtree.index.Index(interleaved=False)
#     marfeats = {}
#     for fid in range(0, eezlyr.GetFeatureCount()):
#         feat = eezlyr.GetFeature(fid)
#         geom = feat.GetGeometryRef()
#         xmin, xmax, ymin, ymax = geom.GetEnvelope()
#         marindex.insert(fid, (xmin, xmax, ymin, ymax))
#         marfeats[fid] = {'feature': feat,
#                          'geom': geom,
#                          'eez': feat.GetFieldAsString(idx_eez),
#                          'mrgid': feat.GetFieldAsString(idx_mg)}
#     return marindex, marfeats
#
# # ...............................................
# def create_terr_index(terrlyr, idx_fips, idx_cnty, idx_st):
#     terrindex = rtree.index.Index(interleaved=False)
#     terrfeats = {}
#     for fid in range(0, terrlyr.GetFeatureCount()):
#         feat = terrlyr.GetFeature(fid)
#         geom = feat.GetGeometryRef()
#         xmin, xmax, ymin, ymax = geom.GetEnvelope()
#         terrindex.insert(fid, (xmin, xmax, ymin, ymax))
#         terrfeats[fid] = {'feature': feat,
#                           'geom': geom,
#                           'fips': feat.GetFieldAsString(idx_fips),
#                           'county': feat.GetFieldAsString(idx_cnty),
#                           'state': feat.GetFieldAsString(idx_st)}
#     return terrindex, terrfeats

# ...............................................
# Main...........................................
# ...............................................

# ...............................................
if __name__ == '__main__':
    inpath, gbif_fname = os.path.split(gbif_interp_file)
    # one level up
    datapth, _ = os.path.split(inpath)
    tmppath = os.path.join(inpath, tmpdir)
    outpath = os.path.join(inpath, outdir)
    outfname = 'datapath'

    # ancillary data for record update
    ancillary_path = os.path.join(datapth, ANCILLARY_DIR)
    terrestrial_shpname = os.path.join(
        ancillary_path, ANCILLARY_FILES['terrestrial']['file'])
    estmeans_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['establishment_means']['file'])
    marine_shpname = os.path.join(
        ancillary_path, ANCILLARY_FILES['marine']['file'])
    itis2_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['itis']['file'])
    resource_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['resource']['file'])
    provider_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['provider']['file'])


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


    points = read_some_points(outfname, testsize)
    terrindex, terrfeats = create_terr_index(terrlyr, idx_fips, idx_cnty, idx_st)
    # ......................


    # ......................
    rtree_start = time.time()
    for i in range(len(points)):
        lon, lat = points[i]
        try:
            (fips, county, state) = get_geofields_with_rtree(lon, lat,
                                                             terrindex, terrfeats)
        except Exception as e:
            print ('Failed on record {} with {}'.format(i, e))

    rtree_stop = time.time()
    rtree_elapsed = rtree_stop - rtree_start
    print(rtree_elapsed)



    # ......................
    ogr_start = time.time()
    for i in range(len(points)):
        lon, lat = points[i]
        try:
            (fips, county, state) = get_geofields_wo_rtree(lon, lat, terrlyr,
                                                           idx_fips, idx_cnty, idx_st)
        except Exception as e:
            print ('Failed on record {} with {}'.format(i, e))

    ogr_stop = time.time()
    ogr_elapsed = ogr_stop - ogr_start
    print(ogr_elapsed)
    # ......................


    # ......................
    rtree_start = time.time()
    for i in range(len(points)):
        lon, lat = points[i]
        try:
            (fips, county, state) = get_geofields_with_rtree(lon, lat,
                                                             terrindex, terrfeats)
        except Exception as e:
            print ('Failed on record {} with {}'.format(i, e))

    rtree_stop = time.time()
    rtree_elapsed = rtree_stop - rtree_start
    print(rtree_elapsed)



    # ......................
    ogr_start = time.time()
    for i in range(len(points)):
        lon, lat = points[i]
        try:
            (fips, county, state) = get_geofields_wo_rtree(lon, lat, terrlyr,
                                                           idx_fips, idx_cnty, idx_st)
        except Exception as e:
            print ('Failed on record {} with {}'.format(i, e))

    ogr_stop = time.time()
    ogr_elapsed = ogr_stop - ogr_start
    print(ogr_elapsed)
    # ......................
