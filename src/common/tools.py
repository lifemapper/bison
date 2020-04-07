import csv
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
from osgeo import ogr, osr
from sys import maxsize
import time

from common.constants import (LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                              LOGFILE_BACKUP_COUNT, EXTRA_VALS_KEY)
from osgeo.ogr import wkbPolygon

REQUIRED_FIELDS = ['STATE_NAME', 'NAME', 'STATE_FIPS', 'CNTY_FIPS', 'PRNAME', 
     'CDNAME', 'CDUID']

# .............................................................................
def getCSVReader(datafile, delimiter, encoding):
    try:
        f = open(datafile, 'r', encoding=encoding) 
        reader = csv.reader(f, delimiter=delimiter)        
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for read'.format(datafile))
    return reader, f

# .............................................................................
def getCSVWriter(datafile, delimiter, encoding, fmode='w'):
    ''' Get a CSV writer that can handle encoding
    
    Args:
        datafile:
        delimiter:
        encoding:
        fmode:
    '''
    if fmode not in ('w', 'a'):
        raise Exception('File mode must be "w" (write) or "a" (append)')
    
    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding) 
        writer = csv.writer(f, delimiter=delimiter)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for write'.format(datafile))
    return writer, f

# .............................................................................
def getCSVDictReader(datafile, delimiter, encoding, fieldnames=None):
    try:
        f = open(datafile, 'r', encoding=encoding)
        if fieldnames is None:
            header = next(f)
            tmpflds = header.split(delimiter)
            fieldnames = [fld.strip() for fld in tmpflds]
        dreader = csv.DictReader(f, fieldnames=fieldnames, 
                                 restkey=EXTRA_VALS_KEY,
                                 delimiter=delimiter)        
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for dict read'.format(datafile))
    return dreader, f

# .............................................................................
def getCSVDictWriter(datafile, delimiter, encoding, fldnames, fmode='w'):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    if fmode not in ('w', 'a'):
        raise Exception('File mode must be "w" (write) or "a" (append)')
    
    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding) 
        writer = csv.DictWriter(f, fieldnames=fldnames, delimiter=delimiter)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for dict write'.format(datafile))
    return writer, f

# .............................................................................
def getLogger(name, fname):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handlers = []
    handlers.append(RotatingFileHandler(fname, maxBytes=LOGFILE_MAX_BYTES, 
                                        backupCount=LOGFILE_BACKUP_COUNT))
    handlers.append(logging.StreamHandler())
    for fh in handlers:
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        log.addHandler(fh)
    return log

# ...............................................
def rotate_logfile(log, logpath, logname=None):
    if log is None:
        if logname is None:
            nm, _ = os.path.splitext(os.path.basename(__file__))
            logname = '{}.{}'.format(nm, int(time.time()))
        logfname = os.path.join(logpath, '{}.log'.format(logname))
        log = getLogger(logname, logfname)
    return log


# ...............................................
def makerow(rec, outfields):
    row = []
    for fld in outfields:
        try:
            if not rec[fld]:
                row.append('')
            else:
                row.append(rec[fld])
        # Add output fields not present in record
        except:
            row.append('')
    return row

# ...............................................
def open_csv_files(infname, delimiter, encoding, infields=None,
                   outfname=None, outfields=None, outdelimiter=None):
    ''' Open CSV data for reading as a dictionary (assumes a header), 
    new output file for writing (rows as a list, not dictionary)
    
    Args: 
        infname: Input CSV filename.  Either a sequence of filenames must 
            be provided as infields or the file must begin with a header
            to set dictionary keys. 
        delimiter: CSV delimiter for input and optionally output 
        encoding: Encoding for input and optionally output 
        infields: Optional ordered list of fieldnames, used when input file
            does not contain a header.
        outfname: Optional output CSV file 
        outdelimiter: Optional CSV delimiter for output if it differs from 
            input delimiter
        outfields: Optional ordered list of fieldnames for output header 
    '''
    # Open incomplete BISON CSV file as input
    csv_dict_reader, inf = getCSVDictReader(infname, delimiter, encoding, 
                                            fieldnames=infields)
    # Optional new BISON CSV output file
    csv_writer = outf = None
    if outfname:
        if outdelimiter is None:
            outdelimiter = delimiter
        csv_writer, outf = getCSVWriter(outfname, outdelimiter, encoding)
        # if outfields are not provided, no header
        if outfields:
            csv_writer.writerow(outfields)
    return (csv_dict_reader, inf, csv_writer, outf)
        

# ...............................................
def getLine(csvreader, recno):
    ''' Return a line while keeping track of the line number and errors
    
    Args:
        csvreader: a csv.reader object opened with a file
        recno: the current record number
    '''
    success = False
    line = None
    while not success and csvreader is not None:
        try:
            line = next(csvreader)
            if line:
                recno += 1
            success = True
        except OverflowError as e:
            recno += 1
            print( 'Overflow on record {}, line {} ({})'
                                 .format(recno, csvreader.line_num, str(e)))
        except StopIteration:
            print('EOF after record {}, line {}'
                                .format(recno, csvreader.line_num))
            success = True
        except Exception as e:
            recno += 1
            print('Bad record on record {}, line {} ({})'
                                .format(recno, csvreader.line_num, e))

    return line, recno

# .............................
def delete_shapefile(shp_filename):
    success = True
    shape_extensions = ['.shp', '.shx', '.dbf', '.prj', '.sbn', '.sbx', '.fbn',
                        '.fbx', '.ain', '.aih', '.ixs', '.mxs', '.atx',
                        '.shp.xml', '.cpg', '.qix']
    if (shp_filename is not None 
        and os.path.exists(shp_filename) and shp_filename.endswith('.shp')):
        base, _ = os.path.splitext(shp_filename)
        similar_file_names = glob.glob(base + '.*')
        try:
            for simfname in similar_file_names:
                _, simext = os.path.splitext(simfname)
                if simext in shape_extensions:
                    os.remove(simfname)
        except Exception as err:
            success = False
            print('Failed to remove {}, {}'.format(simfname, str(err)))
    return success

# # .............................
# def _fill_ogr_feature(feat, feat_vals, feature_attributes):
#     # Fill the fields
#     for i in range(len(feature_attributes)):
#         fldname = feature_attributes[i][0]
#         val = feat_vals[i]
#         if fldname == 'geom':
#             geom = ogr.CreateGeometryFromWkt(val)
#             feat.SetGeometryDirectly(geom)
#         elif val is not None and val != 'None':
#             feat.SetField(fldname, val)
            
# .............................................................................
def _create_empty_dataset(out_shp_filename, feature_attributes, ogr_type, 
                          epsg_code, overwrite=True):
    """ Create an empty ogr dataset given a set of feature attributes
    
    Args:
        out_shp_filename: filename for output data
        feature_attributes: an ordered list of feature_attributes.  
            Each feature_attribute is a tuple of (field name, field type (OGR))
        ogr_type: OGR constant indicating the type of geometry for the dataset
        epsg_code: EPSG code of the spatial reference system (SRS)
        overwrite: boolean indicating if pre-existing data should be deleted.
    
    """
    success = False
    if overwrite:
        delete_shapefile(out_shp_filename)
    elif os.path.isfile(out_shp_filename):
        print(('Dataset exists: {}'.format(out_shp_filename)))
        return success
    
    try:
        # Create the file object, a layer, and attributes
        target_srs = osr.SpatialReference()
        target_srs.ImportFromEPSG(epsg_code)
        drv = ogr.GetDriverByName('ESRI Shapefile')

        dataset = drv.CreateDataSource(out_shp_filename)
        if dataset is None:
            raise Exception(
                'Dataset creation failed for {}'.format(out_shp_filename))

        lyr = dataset.CreateLayer(
            dataset.GetName(), geom_type=ogr_type, srs=target_srs)
        if lyr is None:
            raise Exception(
                'Layer creation failed for {}.'.format(out_shp_filename))

        # Define the fields
        for (fldname, fldtype) in feature_attributes:
            if fldname == 'geometries':
                fld_defn = ogr.FieldDefn('geom', ogr.OFTString)
            else:
                fld_defn = ogr.FieldDefn(fldname, fldtype)
                # Special case to handle long names
                if (fldname.endswith('name') and fldtype == ogr.OFTString):
                    fld_defn.SetWidth(255)
                return_val = lyr.CreateField(fld_defn)
                if return_val != 0:
                    raise Exception(
                        'CreateField failed for {} in {}'.format(
                            fldname, out_shp_filename))
            
    except Exception as e:
        print('Failed to create shapefile {}'.format(out_shp_filename), e)
                    
    return dataset, lyr

# .............................................................................
def _calc_newfield_values(feat_vals):
    state = cnty = fips = ''
    try:
        state = feat_vals['STATE_NAME']
    except:
        try:
            tmp = feat_vals['PRNAME']
            state = tmp.replace(' Canada', '')
        except:
            print('Failed to fill B_STATE')
    try:
        cnty = feat_vals['NAME']
    except:
        try:
            cnty = feat_vals['CDNAME']
        except:
            print('Failed to fill B_COUNTY')
    try:
        fips = '{}{}'.format(
            feat_vals['STATE_FIPS'], feat_vals['CNTY_FIPS'])
    except:
        try:
            fips = feat_vals['CDUID']
        except:
            print('Failed to fill B_FIPS')
    return {'B_STATE': state, 'B_COUNTY': cnty, 'B_FIPS': fips}

# # .............................................................................
# def _make_feature(new_layer, new_layer_def, feat_vals, geom, newfield_mapping):
#     # create a new feature
#     newfeat = ogr.Feature(new_layer_def)
#     try:
#         # put old dataset values into old fieldnames
#         for fldname, fldval in feat_vals.items():
#             newfeat.SetField(fldname, fldval)
#         # put new calculated values into new fieldnames
#         calcvals = _calc_newfield_values(feat_vals, geom, newfield_mapping)
#         for calcname, calcval in calcvals.items():
#             newfeat.SetField(calcname, calcval)
#         # set geometry
#         newfeat.SetGeometryDirectly(geom)
#     except Exception as e:
#         print('Failed to fill feature, e = {}'.format(e))
#     else:
#         # Create new feature, setting FID, in this layer
#         new_layer.CreateFeature(newfeat)
#         newfeat.Destroy() 
           
# .............................................................................
def write_shapefile(new_dataset, new_layer, feature_sets):
    """ Write a shapefile given a set of features, attribute
    
    Args:
        new_dataset: an OGR dataset object for the new shapefile
        new_layer: an OGR layer object with feature
        newfield_mapping = 
    """
    new_layer_def = new_layer.GetLayerDefn()
    # for each set of features
    for feats in feature_sets:
        print('Starting feature set with {} features'.format(len(feats)))
        # for each feature
        for oldfid, feat_vals in feats.items():
            # create one or more features
            simple_geoms = feat_vals['geometries']
            print('Starting feature with {} geometries'.format(len(simple_geoms)))
            for geom in simple_geoms:
                try:
                    # create a new feature
                    newfeat = ogr.Feature(new_layer_def)
                    # put old dataset values into old fieldnames
                    for fldname, fldval in feat_vals.items():
                        if fldname != 'geometries' and fldval is not None:
                            newfeat.SetField(fldname, fldval)
                    # put new calculated values into new fieldnames
                    calcvals = _calc_newfield_values(feat_vals)
                    for calcname, calcval in calcvals.items():
                        newfeat.SetField(calcname, calcval)
                    # set geometry
                    newfeat.SetGeomField('geom', geom)
                except Exception as e:
                    print('Failed to fill feature, e = {}'.format(e))
                else:
                    # Create new feature, setting FID, in this layer
                    new_layer.CreateFeature(newfeat)
                    newfeat.Destroy()

    # Close and flush to disk
    new_dataset.Destroy()

# .............................................................................
def _read_complex_shapefile(in_shp_filename):
    ogr.RegisterAll()
    drv = ogr.GetDriverByName('ESRI Shapefile')
    try:
        dataset = drv.Open(in_shp_filename)
    except Exception:
        print('Unable to open {}'.format(in_shp_filename))
        raise

    try:
        lyr = dataset.GetLayer(0)
    except Exception:
        print('Unable to get layer from {}'.format(in_shp_filename))
        raise 

    (min_x, max_x, min_y, max_y) = lyr.GetExtent()
    bbox = (min_x, min_y, max_x, max_y)
    lyr_def = lyr.GetLayerDefn()
    fld_count = lyr_def.GetFieldCount()

    # Read Fields (indexes start at 0)
    feat_attrs = []
    for i in range(fld_count):
        fld = lyr_def.GetFieldDefn(i)
        fldname = fld.GetNameRef()
        # ignore these fields
        if fldname not in ('JSON', 'EXTENT'):
            feat_attrs.append((fld.GetNameRef(), fld.GetType()))
    
    # Read Features
    feats = {}
    try:
        old_feat_count = lyr.GetFeatureCount()
        new_feat_count = 0
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            geom = feat.geometry()
            # Save centroid of original polygon
            centroid = geom.Centroid()
            feat_vals = {
                'B_CENTROID': centroid.ExportToWkt() 
                }
            for (fldname, _) in feat_attrs:
                try:
                    val = feat.GetFieldAsString(fldname)
                except:
                    val = ''
                    if fldname in REQUIRED_FIELDS:
                        print('Failed to read value in {}, FID {}'.format(fldname, fid))
                feat_vals[fldname] = val
            feat_geoms = []
            if geom.GetGeometryName() == 'MULTIPOLYGON':
                for i in range(geom.GetGeometryCount()):
                    subgeom = geom.GetGeometryRef(i)
                    subname = subgeom.GetGeometryName()
                    if subname == 'POLYGON':
                        feat_geoms.append(subgeom)
                    else:
                        print('{} (non-polygon) subgeom, simple {}, count {}'.format(
                            subname, subgeom.IsSimple(), subgeom.GetGeometryCount()))
            # Add one or more geometries to feature
            if len(feat_geoms) == 0:
                feat_geoms.append(geom)
            feat_vals['geometries'] = feat_geoms
            new_feat_count += len(feat_geoms)    
            feats[fid] = feat_vals
        print('Read {} features into {} simple features'.format(
            old_feat_count, new_feat_count))

    except Exception as e:
        raise Exception('Failed to read features from {} ({})'.format(
            out_shp_filename, e))
        
    finally:
        lyr = None
        dataset = None
        
    return feats, feat_attrs, bbox

# .............................................................................
def merge_simplify_shapefiles(in_shp_filename1, in_shp_filename2, 
                              newfield_mapping, out_shp_filename):
    ''' Merge 2 shapefiles, simplifying multipolygons into simple polygons with
    the same attribute values.
    
    Args:
        in_shp_filename1:
        in_shp_filename2: 
        newfield_mapping: 
        out_shp_filename:
    '''
    epsg_code = 4326
    # Open input shapefiles, read layer def
    feats1, feat_attrs1, bbox1 = _read_complex_shapefile(in_shp_filename1)
    feats2, feat_attrs2, bbox2 = _read_complex_shapefile(in_shp_filename2)
#     feat_attrs1, bbox1 = _read_shapefile_fields(in_shp_filename1)
#     feat_attrs2, bbox2 = _read_shapefile_fields(in_shp_filename2)
#     input_data = {in_shp_filename1: feat_attrs1, in_shp_filename2: feat_attrs2}
    # ----------------- Merge attributes -----------------
    out_feat_attrs = []
    fnames1 = []
    for (fname,ftype) in feat_attrs1:
        out_feat_attrs.append((fname,ftype))
        fnames1.append(fname)
    for (fname,ftype) in feat_attrs2:
        if fname in fnames1:
            fname = fname+'2'
        out_feat_attrs.append((fname,ftype))
    # Add new attributes
    for new_fldname, new_fldtype in newfield_mapping.items():
        out_feat_attrs.append((new_fldname, new_fldtype))
        
    new_bbox = (min(bbox1[0], bbox2[0]), min(bbox1[1], bbox2[1]),
                max(bbox1[2], bbox2[2]), max(bbox1[3], bbox2[3]))
        
    # ----------------- Create structure -----------------
    out_dataset, out_layer = _create_empty_dataset(
        out_shp_filename, out_feat_attrs, wkbPolygon, epsg_code, new_bbox)
    
    # ----------------- Write old feats to new layer  -----------------
    write_shapefile(out_dataset, out_layer, [feats1, feats2])    
#     merge_shapefiles(out_dataset, out_layer, input_data)


# ...............................................
if __name__ == '__main__':
    pth = '/tank/data/bison/2019/ancillary'
    sfname1 = 'us_counties/us_counties.shp'
    sfname2 = 'can_counties/can_counties.shp'
    outfname = 'us_can_boundaries_centroids.shp'
    
    in_shp_filename1 = os.path.join(pth, sfname1)
    in_shp_filename2 = os.path.join(pth, sfname2)
    out_shp_filename = os.path.join(pth, outfname)
    newfield_mapping = {
        'B_STATE': ogr.OFTString,
        'B_COUNTY': ogr.OFTString,
        'B_FIPS': ogr.OFTString,
        'B_CENTROID': ogr.OFTString
        }
    
    merge_simplify_shapefiles(in_shp_filename1, in_shp_filename2, 
                              newfield_mapping, out_shp_filename)
#     if not os.path.exists(out_shp_filename):                                
#         merge_simplify_shapefiles(in_shp_filename1, in_shp_filename2, 
#                                   newfield_mapping, out_shp_filename)
#     else:
#         (_, _, _) = _read_complex_shapefile(out_shp_filename)
    
    
    