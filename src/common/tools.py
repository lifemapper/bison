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

# .............................
def _fill_ogr_feature(feat, feat_vals, feature_attributes):
    # Fill the fields
    for i in range(len(feature_attributes)):
        fldname = feature_attributes[i][0]
        val = feat_vals[i]
        if fldname == 'geom':
            geom = ogr.CreateGeometryFromWkt(val)
            feat.SetGeometryDirectly(geom)
        elif val is not None and val != 'None':
            feat.SetField(fldname, val)
            
# .............................................................................
def _create_empty_dataset(out_shp_filename, feature_attributes, ogr_type, 
                          epsg_code, overwrite=False):
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
            if fldname != 'geom':
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
def _calc_newfield_values(feat_vals, newfield_mapping):
    calcvals = {}
    for newfldname in newfield_mapping.keys():
        if newfldname == 'B_STATE':
            try:
                calcval = feat_vals['STATE_NAME']
            except:
                try:
                    tmp = feat_vals['PRNAME']
                    calcval = tmp.replace(' Canada', '')
                except:
                    print('Failed to fill B_STATE')
        elif newfldname == 'B_COUNTY':
            try:
                calcval = feat_vals['NAME']
            except:
                try:
                    calcval = feat_vals['CDNAME']
                except:
                    print('Failed to fill B_COUNTY')
        elif newfldname == 'B_FIPS':
            try:
                calcval = '{}{}'.format(
                    feat_vals['STATE_FIPS'], feat_vals['CNTY_FIPS'])
            except:
                try:
                    calcval = feat_vals['CDUID']
                except:
                    print('Failed to fill B_FIPS')
        elif newfldname == 'B_CENTROID':
            geom = feat_vals['geom']
            centroid = geom.Centroid()
            calcval = centroid.ExportToWkt()
            
        calcvals[newfldname] = calcval
        
    return calcvals


# .............................................................................
def write_shapefile(new_dataset, new_layer, newfield_mapping, feats_list):
    """ Write a shapefile given a set of features, attribute
    
    Args:
        new_dataset: an OGR dataset object for the new shapefile
        new_layer: an OGR layer object with feature
        newfield_mapping = 
    """
    new_layer_def = new_layer.GetLayerDefn()
    # for each set of features
    for feats in feats_list:
        # for each feature
        for oldfid, feat_vals in feats.keys():
            # create a new feature
            newfeat = ogr.Feature(new_layer_def)
            try:
                # put old dataset values into old fieldnames
                for fldname, fldval in feat_vals:
                    newfeat.SetField(fldname, fldval)
                # put new calculated values into new fieldnames
                calcvals = _calc_newfield_values(feat_vals, newfield_mapping)
                for calcname, calcval in calcvals.items():
                    newfeat.SetField(calcname, calcval)
                # set geometry
                newfeat.SetGeometryDirectly(feat_vals['geom'])                
            except Exception as e:
                print('Failed to fill feature, e = {}'.format(e))
            else:
                # Create new feature, setting FID, in this layer
                new_layer.CreateFeature(newfeat)
                newfeat.Destroy()
    # Close and flush to disk
    new_dataset.Destroy()

# .............................................................................
def _read_shapefile(in_shp_filename):
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
        feat_attrs.append((fld.GetNameRef(), fld.GetType()))
    # Add geom WKT as last field
    feat_attrs.append('geom')
    
    # Read Features
    feats = {}
    try:
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            feat_vals = {}
            for (fldname, _) in feat_attrs:
                val = feat.GetFieldAsString(fldname)
                feat_vals[fldname] = val
            # Add geom WKT as last field
            feat_vals.append(feat.geometry())
            feats[fid] = feat_vals

    except Exception as e:
        raise Exception('Failed to read features from {} ({})'.format(
            out_shp_filename, e))
        
    return lyr, feats, feat_attrs, bbox

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
    lyr1, feats1, feat_attrs1, bbox1 = _read_shapefile(in_shp_filename1)
    lyr2, feats2, feat_attrs2, bbox2 = _read_shapefile(in_shp_filename2)
    # ----------------- Merge attributes -----------------
    new_feat_attrs = []
    fnames1 = []
    for (fname,ftype) in feat_attrs1:
        new_feat_attrs.append((fname,ftype))
        fnames1.append(fname)
    for (fname,ftype) in feat_attrs2:
        if fname in fnames1:
            fname = fname+'2'
        new_feat_attrs.append((fname,ftype))
    # Add new attributes
    for new_fldname in newfield_mapping.keys():
        new_fldtype = newfield_mapping['ogr_field_type']
        new_feat_attrs.append(new_fldname, new_fldtype)
        
    # ----------------- Merge features  -----------------
    
    new_dataset, new_layer = _create_empty_dataset(
        out_shp_filename, new_feat_attrs, wkbPolygon, epsg_code)
    
    new_feats = []
    newid = 0
    for fid, f_vals in feats1.items():
        newid += 1
        for (new_fldname, new_fldtype) in new_feat_attrs:
            
        
    
    # ----------------- Write -----------------
    write_shapefile(new_dataset, new_layer, newfield_mapping, [feats1, feats2])    


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
        'B_STATE': {
            'ogr_field_type': ogr.OFTString,
            in_shp_filename1: 'STATE_NAME',
            in_shp_filename2: ('replace', 'PRNAME', ' Canada', '')
            },
        'B_COUNTY': {
            'ogr_field_type': ogr.OFTString,
            in_shp_filename1: 'NAME',
            in_shp_filename2: 'CDNAME'
            },
        'B_FIPS': {
            'ogr_field_type': ogr.OFTString,
            in_shp_filename1: ('concat', 'STATE_FIPS', 'CNTY_FIPS'),
            in_shp_filename2: 'CDUID'
            },
        'B_CENTROID': {
            'ogr_field_type': ogr.OFTString,
            in_shp_filename1: ('geom_to_wkt', 'centroid', 'geom'),
            in_shp_filename2: ('geom_to_wkt', 'centroid', 'geom')
            }
        }
                                    
    merge_simplify_shapefiles(in_shp_filename1, in_shp_filename2, 
                              newfield_mapping, out_shp_filename)
    
    
    
    