import csv
import glob
import logging
from logging.handlers import RotatingFileHandler
from multiprocessing import cpu_count
import os
from osgeo import ogr, osr
import rtree
import subprocess
from sys import maxsize
import time

from common.constants import (LOG_FORMAT, LOG_DATE_FORMAT, LOGFILE_MAX_BYTES,
                              LOGFILE_BACKUP_COUNT, EXTRA_VALS_KEY)

REQUIRED_FIELDS = ['STATE_NAME', 'NAME', 'STATE_FIPS', 'CNTY_FIPS', 'PRNAME', 
     'CDNAME', 'CDUID']
CENTROID_FIELD = 'B_CENTROID'

# .............................................................................
def get_line_count(filename):
    """ find total number lines in a file """
    cmd = "wc -l {}".format(repr(filename))
    info, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    temp = info.split(b'\n')[0]
    line_count = int(temp.split()[0])
    return line_count

# .............................................................................
def get_process_count():
    return cpu_count() - 2

# .............................................................................
def _find_chunk_files(big_csv_filename, out_csv_filename):
    """ Finds multiple smaller input csv files from a large input csv file, 
    if they exist, and return these filenames, paired with output filenames 
    for the results of processing these files. """
    cpus2use = get_process_count()
    in_base_filename, _ = os.path.splitext(big_csv_filename)
    # Construct provider filenames from outfilename and separate in/out paths
    out_fname_noext, ext = os.path.splitext(out_csv_filename)
    outpth, basename = os.path.split(out_fname_noext)
    out_base_filename = os.path.join(outpth, basename)
        
    total_lines = get_line_count(big_csv_filename) - 1
    chunk_size = int(total_lines / cpus2use)
    
    csv_filename_pairs = []
    start = 1
    stop = chunk_size
    while start <= total_lines:
        in_filename = '{}_chunk-{}-{}{}'.format(in_base_filename, start, stop, ext)
        out_filename =  '{}_chunk-{}-{}{}'.format(out_base_filename, start, stop, ext)
        if os.path.exists(in_filename):
            csv_filename_pairs.append((in_filename, out_filename))
        else:
            # Return basenames if files are not present
            csv_filename_pairs = [(in_base_filename, out_base_filename)]
            print('Missing file {}'.format(in_filename))
            break
        start = stop + 1
        stop = start + chunk_size - 1
    return csv_filename_pairs, chunk_size

# .............................................................................
def get_chunk_files(big_csv_filename, out_csv_filename):
    """ Creates multiple smaller input csv files from a large input csv file, and 
    return these filenames, paired with output filenames for the results of 
    processing these files. """
    csv_filename_pairs, chunk_size = _find_chunk_files(
        big_csv_filename, out_csv_filename)
    # First pair is existing files OR basenames
    if os.path.exists(csv_filename_pairs[0][0]):
        header = get_header(big_csv_filename)
        return csv_filename_pairs, header
    else:
        in_base_filename = csv_filename_pairs[0][0]
        out_base_filename = csv_filename_pairs[0][1]
    
    csv_filename_pairs = []
    try:
        bigf = open(big_csv_filename, 'r', encoding='utf-8')
        header = bigf.readline()
        line = bigf.readline()
        curr_recno = 1
        while line != '':
            # Reset vars for next chunk
            start = curr_recno
            stop = start + chunk_size - 1
            in_filename = '{}_chunk-{}-{}.csv'.format(in_base_filename, start, stop)
            out_filename =  '{}_chunk-{}-{}.csv'.format(out_base_filename, start, stop)
            csv_filename_pairs.append((in_filename, out_filename))
            try:
                # Start writing the smaller file
                inf = open(in_filename, 'w', encoding='utf-8')
                inf.write('{}'.format(header))
                while curr_recno <= stop:
                    if line != '':
                        inf.write('{}'.format(line))
                        line = bigf.readline()
                        curr_recno += 1
                    else:
                        curr_recno = stop + 1
            except Exception as inner_err:
                print('Failed in inner loop {}'.format(inner_err))
                raise
            finally:
                inf.close()
    except Exception as outer_err:
        print('Failed to do something {}'.format(outer_err))
        raise
    finally:
        bigf.close()
    
    return csv_filename_pairs, header

# .............................................................................
def get_header(filename):
    """ find fieldnames from the first line of a CSV file """
    header = None
    try:
        f = open(filename, 'r', encoding='utf-8')
        header = f.readline()
    except Exception as e:
        print('Failed to read first line of {}: {}'.format(filename, e))
    finally:
        f.close()
    return header

# .............................................................................
def get_csv_reader(datafile, delimiter, encoding):
    try:
        f = open(datafile, 'r', encoding=encoding) 
        reader = csv.reader(f, delimiter=delimiter, escapechar='\\', 
                            quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for read'.format(datafile))
    return reader, f

# .............................................................................
def get_csv_writer(datafile, delimiter, encoding, fmode='w'):
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
        writer = csv.writer(
            f, escapechar='\\', delimiter=delimiter, quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for write'.format(datafile))
    return writer, f

# .............................................................................
def get_csv_dict_reader(datafile, delimiter, encoding, fieldnames=None, 
                        ignore_quotes=True):
    '''
    ignore_quotes: no special processing of quote characters
    '''
    try:
        f = open(datafile, 'r', encoding=encoding)
        if fieldnames is None:
            header = next(f)
            tmpflds = header.split(delimiter)
            fieldnames = [fld.strip() for fld in tmpflds]
        if ignore_quotes:
            dreader = csv.DictReader(
                f, fieldnames=fieldnames, quoting=csv.QUOTE_NONE,
                escapechar='\\', restkey=EXTRA_VALS_KEY, delimiter=delimiter)
        else:
            dreader = csv.DictReader(
                f, fieldnames=fieldnames, restkey=EXTRA_VALS_KEY, 
                escapechar='\\', delimiter=delimiter)
            
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for dict read'.format(datafile))
    return dreader, f

# .............................................................................
def get_csv_dict_writer(datafile, delimiter, encoding, fldnames, fmode='w'):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    if fmode not in ('w', 'a'):
        raise Exception('File mode must be "w" (write) or "a" (append)')
    
    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding) 
        writer = csv.DictWriter(f, fieldnames=fldnames, delimiter=delimiter,
                                escapechar='\\', quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception('Failed to read or open {}, ({})'
                        .format(datafile, str(e)))
    else:
        print('Opened file {} for dict write'.format(datafile))
    return writer, f

# .............................................................................
def get_logger(name, fname):
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
        log = get_logger(logname, logfname)
    return log


# ...............................................
def makerow(rec, outfields):
    row = []
    for fld in outfields:
        try:
            val = rec[fld]
            if val in (None, 'None'):
                row.append('')
            else:
                if isinstance(val, str) and val.startswith('\"'):
                    val = val.strip('\"')
                row.append(val)
        # Add output fields not present in record
        except:
            row.append('')
    return row


# ...............................................
def open_csv_files(infname, delimiter, encoding, ignore_quotes=True, 
                   infields=None, outfname=None, outfields=None, 
                   outdelimiter=None):
    ''' Open CSV data for reading as a dictionary (assumes a header), 
    new output file for writing (rows as a list, not dictionary)
    
    Args: 
        infname: Input CSV filename.  Either a sequence of filenames must 
            be provided as infields or the file must begin with a header
            to set dictionary keys. 
        delimiter: CSV delimiter for input and optionally output 
        encoding: Encoding for input and optionally output 
        ignore_quotes: if True, QUOTE_NONE
        infields: Optional ordered list of fieldnames, used when input file
            does not contain a header.
        outfname: Optional output CSV file 
        outdelimiter: Optional CSV delimiter for output if it differs from 
            input delimiter
        outfields: Optional ordered list of fieldnames for output header 
    '''
    # Open incomplete BISON CSV file as input
    csv_dict_reader, inf = get_csv_dict_reader(
        infname, delimiter, encoding, fieldnames=infields, 
        ignore_quotes=ignore_quotes)
    # Optional new BISON CSV output file
    csv_writer = outf = None
    if outfname:
        if outdelimiter is None:
            outdelimiter = delimiter
        csv_writer, outf = get_csv_writer(outfname, outdelimiter, encoding)
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
            fld_defn = ogr.FieldDefn(fldname, fldtype)
            # Special case to handle long names
            if (fldname.endswith('name') and fldtype == ogr.OFTString):
                fld_defn.SetWidth(255)
            return_val = lyr.CreateField(fld_defn)
            if return_val != 0:
                raise Exception(
                    'CreateField failed for {} in {}'.format(
                        fldname, out_shp_filename))
        print('Created empty dataset with {} fields'.format(
            len(feature_attributes)))            
    except Exception as e:
        print('Failed to create shapefile {}'.format(out_shp_filename), e)
                    
    return dataset, lyr

# .............................................................................
def _calc_newfield_values(feat_vals, calc_fldnames):
    calc_vals = {}
    state = cnty = fips = None
    for fldname in calc_fldnames:
        if fldname ==  'B_STATE':
            try:
                state = feat_vals['STATE_NAME']
            except:
                try:
                    tmp = feat_vals['PRNAME']
                    state = tmp.replace(' Canada', '')
                except:
                    print('Failed to fill B_STATE')
            calc_vals['B_STATE'] = state
        elif fldname == 'B_COUNTY':
            try:
                cnty = feat_vals['NAME']
            except:
                try:
                    cnty = feat_vals['CDNAME']
                except:
                    print('Failed to fill B_COUNTY')
            calc_vals['B_COUNTY'] = cnty
        elif fldname == 'B_FIPS':
            try:
                fips = '{}{}'.format(
                    feat_vals['STATE_FIPS'], feat_vals['CNTY_FIPS'])
            except:
                try:
                    fips = feat_vals['CDUID']
                except:
                    print('Failed to fill B_FIPS')
            calc_vals['B_FIPS'] = fips
        else:
            print('Not calculating field {}'.format(fldname))
    return calc_vals

# .............................................................................
def get_clustered_spatial_index(shp_filename):
    pth, basename = os.path.split(shp_filename)
    idxname, _ = os.path.splitext(basename)
    idx_filename = os.path.join(pth, idxname)

    if not(os.path.exists(idx_filename+'.dat')):
        # Create spatial index
        prop = rtree.index.Property()
        prop.set_filename(idx_filename)
        
        driver = ogr.GetDriverByName("ESRI Shapefile")
        datasrc = driver.Open(shp_filename, 0)
        lyr = datasrc.GetLayer()
        spindex = rtree.index.Index(idx_filename, interleaved=False, properties=prop)
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            geom = feat.geometry()
            wkt = geom.ExportToWkt()
            # OGR returns xmin, xmax, ymin, ymax
            xmin, xmax, ymin, ymax = geom.GetEnvelope()
            # Rtree takes xmin, xmax, ymin, ymax IFF interleaved = False
            spindex.insert(fid, (xmin, xmax, ymin, ymax), obj=wkt)
        # Write spatial index
        spindex.close()
    else:
        spindex = rtree.index.Index(idx_filename, interleaved=False)
    return spindex

# .............................................................................
def _refine_intersect(gc_wkt, poly, new_layer, feat_vals):
    newfeat_count = 0
    # select only the intersections
    gridcell = ogr.CreateGeometryFromWkt(gc_wkt)
    if poly.Intersects(gridcell):
        intersection = poly.Intersection(gridcell)
        itxname = intersection.GetGeometryName()
        
        # Split polygon/gridcell intersection into 1 or more simple polygons
        itx_polys = []
        if itxname == 'POLYGON':
            itx_polys.append(intersection)
        elif itxname in ('MULTIPOLYGON', 'GEOMETRYCOLLECTION'):
            for i in range(intersection.GetGeometryCount()):
                subgeom = intersection.GetGeometryRef(i)
                subname = subgeom.GetGeometryName()
                if subname == 'POLYGON':
                    itx_polys.append(subgeom)
                else:
                    print('{} intersection subgeom, simple {}, count {}'.format(
                        subname, subgeom.IsSimple(), subgeom.GetGeometryCount()))
        else:
            print('{} intersection geom, simple {}, count {}'.format(
                itxname, intersection.IsSimple(), intersection.GetGeometryCount()))

        # Make a feature from each simple polygon
        for ipoly in itx_polys:
            try:
                newfeat = ogr.Feature(new_layer.GetLayerDefn())
                # Makes a copy of the ipoly for the new feature
                newfeat.SetGeometry(ipoly)
                # put values into fieldnames
                for fldname, fldval in feat_vals.items():
                    if fldname != 'geometries' and fldval is not None:
                        newfeat.SetField(fldname, fldval)
            except Exception as e:
                print('      Failed to fill feature, e = {}'.format(e))
            else:
                # Create new feature, setting FID, in this layer
                new_layer.CreateFeature(newfeat)
                newfeat.Destroy()
                newfeat_count += 1
    return newfeat_count

# .............................................................................
def intersect_write_shapefile(new_dataset, new_layer, feats, grid_index):
    feat_count = 0
    # for each feature
    print ('Loop through {} poly features for intersection'.format(len(feats)))
    for fid, feat_vals in feats.items():
        curr_count = 0
        # create new feature for every simple geometry
        simple_wkts = feat_vals['geometries']
        print ('  Loop through {} simple features in poly'.format(
            len(simple_wkts)))
        for wkt in simple_wkts:
            # create a new feature
            simple_geom = ogr.CreateGeometryFromWkt(wkt)
            gname = simple_geom.GetGeometryName()
            if gname != 'POLYGON':
                print ('    Discard invalid {} subgeometry'.format(gname))
            else:
                # xmin, xmax, ymin, ymax
                xmin, xmax, ymin, ymax = simple_geom.GetEnvelope()
                hits = list(grid_index.intersection((xmin, xmax, ymin, ymax), 
                                                    objects=True))
                print ('    Loop through {} roughly intersected gridcells'.format(len(hits)))
                for item in hits:
                    gc_wkt = item.object
                    newfeat_count = _refine_intersect(
                        gc_wkt, simple_geom, new_layer, feat_vals)
                    curr_count += newfeat_count
#                     print ('  Created {} new features for simplified poly'.format(
#                         newfeat_count))
        print ('  Created {} new features for primary poly'.format(curr_count))
        feat_count += curr_count
    print ('Created {} new features from intersection'.format(feat_count))
    # Close and flush to disk
    new_dataset.Destroy()

# .............................................................................
def write_shapefile(new_dataset, new_layer, feature_sets, calc_nongeo_fields):
    """ Write a shapefile given a set of features, attribute
    
    Args:
        new_dataset: an OGR dataset object for the new shapefile
        new_layer: an OGR layer object with feature
        newfield_mapping = 
    """
    feat_count = 0
    new_layer_def = new_layer.GetLayerDefn()
    # for each set of features
    for feats in feature_sets:
        # for each feature
        for oldfid, feat_vals in feats.items():
            # create new feature for every simple geometry
            simple_geoms = feat_vals['geometries']
            for wkt in simple_geoms:
                try:
                    # create a new feature
                    newfeat = ogr.Feature(new_layer_def)
                    poly = ogr.CreateGeometryFromWkt(wkt)
                    # New geom can be assigned directly to new feature
                    newfeat.SetGeometryDirectly(poly)
                    # put old dataset values into old fieldnames
                    for fldname, fldval in feat_vals.items():
                        if fldname != 'geometries' and fldval is not None:
                            newfeat.SetField(fldname, fldval)
                    # put new calculated values into new fieldnames
                    calcvals = _calc_newfield_values(feat_vals, calc_nongeo_fields)
                    for calcname, calcval in calcvals.items():
                        newfeat.SetField(calcname, calcval)
                except Exception as e:
                    print('Failed to fill feature, e = {}'.format(e))
                else:
                    # Create new feature, setting FID, in this layer
                    new_layer.CreateFeature(newfeat)
                    newfeat.Destroy()
                    feat_count += 1
        print('Wrote {} records from feature set'.format(feat_count))
        feat_count = 0

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
        fldtype = fld.GetType()
        # ignore these fields
        if fldname not in ('JSON', 'EXTENT', 'ALAND', 'AWATER', 'Area_m2'):
            if fldname == 'MRGID':
                fldtype = ogr.OFTInteger
            feat_attrs.append((fldname, fldtype))
    
    # Read Features
    feats = {}
    try:
        old_feat_count = lyr.GetFeatureCount()
        new_feat_count = 0
        for fid in range(0, lyr.GetFeatureCount()):
            feat = lyr.GetFeature(fid)
            feat_vals = {}
            for (fldname, _) in feat_attrs:
                try:
                    val = feat.GetFieldAsString(fldname)
                except:
                    val = ''
                    if fldname in REQUIRED_FIELDS:
                        print('Failed to read value in {}, FID {}'.format(
                            fldname, fid))
                feat_vals[fldname] = val
            # Save centroid of original polygon
            geom = feat.geometry()
            geom_name = geom.GetGeometryName()
            centroid = geom.Centroid()
            feat_vals[CENTROID_FIELD] = centroid.ExportToWkt() 
            # Split multipolygon into 1 record - 1 simple polygon
            feat_wkts = []
            if geom_name == 'POLYGON':
                feat_wkts.append(geom.ExportToWkt())
            elif geom_name in ('MULTIPOLYGON', 'GEOMETRYCOLLECTION'):
                for i in range(geom.GetGeometryCount()):
                    subgeom = geom.GetGeometryRef(i)
                    subname = subgeom.GetGeometryName()
                    if subname == 'POLYGON':
                        feat_wkts.append(subgeom.ExportToWkt())
                    else:
                        print('{} subgeom, simple {}, count {}'.format(
                            subname, subgeom.IsSimple(), subgeom.GetGeometryCount()))
            else:
                print('{} primary geom, simple {}, count {}'.format(
                    geom_name, geom.IsSimple(), geom.GetGeometryCount()))
            # Add one or more geometries to feature
            if len(feat_wkts) == 0:
                feat_wkts.append(geom.ExportToWkt())
            feat_vals['geometries'] = feat_wkts
            new_feat_count += len(feat_wkts)
            feats[fid] = feat_vals
        print('Read {} features into {} simple features'.format(
            old_feat_count, new_feat_count))

    except Exception as e:
        raise Exception('Failed to read features from {} ({})'.format(
            in_shp_filename, e))
        
    finally:
        lyr = None
        dataset = None
        
    return feats, feat_attrs, bbox

# .............................................................................
def simplify_merge_polygon_shapefiles(in_shp_filenames, calc_fields, out_shp_filename):
    ''' Merge one or more shapefiles, simplifying multipolygons into simple polygons with
    the same attribute values.
    
    Args:
        in_shp_filenames: list of one or more input shapefiles 
        newfield_mapping: dictionary of new fields, fieldtypes
        out_shp_filename: output filename
    '''
    epsg_code = 4326
    # Open input shapefiles, read layer def
    features_lst = []
    feat_attrs_lst = []
    bboxes = []
    for shp_fname in in_shp_filenames:
        # Calculate B_CENTROID and save values of original polygon/feature
        feats, feat_attrs, bbox = _read_complex_shapefile(shp_fname)
        features_lst.append(feats)
        feat_attrs_lst.append(feat_attrs)
        bboxes.append(bbox)

    # ......................... Merge attributes .........................
    out_feat_attrs = []
    new_fldnames = []
    for i in range(len(feat_attrs_lst)):
        feat_attrs = feat_attrs_lst[i]
        for (fname,ftype) in feat_attrs:
            if fname in new_fldnames:
                fname = fname + str(i)
            new_fldnames.append(fname)
            out_feat_attrs.append((fname,ftype))
    # Add new attributes including B_CENTROID
    for calc_fldname, calc_fldtype in calc_fields.items():
        out_feat_attrs.append((calc_fldname, calc_fldtype))
        new_fldnames.append(calc_fldname)

    # ......................... ? bbox .........................
    new_bbox = (min([b[0] for b in bboxes]), min([b[1] for b in bboxes]),
                max([b[2] for b in bboxes]), max([b[3] for b in bboxes]))
        
    # ......................... Create structure .........................
    out_dataset, out_layer = _create_empty_dataset(
        out_shp_filename, out_feat_attrs, ogr.wkbPolygon, epsg_code, 
        overwrite=True)
    
    # ......................... Write old feats to new layer  .........................
    # Calculate non-geometric fields
    # Write one or more new features for each original feature
    calc_nongeo_fields = [k for k in calc_fields.keys() if k != CENTROID_FIELD] 
    write_shapefile(
        out_dataset, out_layer, features_lst, calc_nongeo_fields)


# .............................................................................
def intersect_polygon_with_grid(primary_shp_filename, grid_shp_filename, 
                                calc_fields, out_shp_filename):
    ''' Intersect a primary shapefile with a grid (or other simple polygon) 
    shapefile, simplifying multipolygons in the primary shapefile into simple 
    polygons. Intersect the simple polygons with gridcells in the second 
    shapefile to further reduce polygon size and complexity.
    
    Args:
        in_shp_filenames: list of one or more input shapefiles 
        grid_shp_filename: dictionary of new fields, fieldtypes
        out_shp_filename: output filename
    '''
    epsg_code = 4326
    # Open input shapefile, read layer def
    feats, feat_attrs, bbox = _read_complex_shapefile(primary_shp_filename)
    
    # Add new attributes including B_CENTROID
    for calc_fldname, calc_fldtype in calc_fields.items():
        feat_attrs.append((calc_fldname, calc_fldtype))
         
    # Get spatial index for grid with WKT for each cell
    grid_index = get_clustered_spatial_index(grid_shp_filename)
 
    # ......................... Create structure .........................
    out_dataset, out_layer = _create_empty_dataset(
        out_shp_filename, feat_attrs, ogr.wkbPolygon, epsg_code, 
        overwrite=True)
       
    # ......................... Intersect polygons .........................
    intersect_write_shapefile(out_dataset, out_layer, feats, grid_index)
        

# ...............................................
if __name__ == '__main__':
    pth = '/tank/data/bison/2019/ancillary'
    
#     # Terrestrial boundaries
#     us_sfname = 'us_counties/us_counties.shp'
#     can_sfname = 'can_counties/can_counties.shp'
#     terr_outfname = 'us_can_boundaries_centroids.shp'
#     
#     in_terr_filenames = [os.path.join(pth, us_sfname), 
#                         os.path.join(pth, can_sfname)]
#     out_terr_filename = os.path.join(pth, terr_outfname)
#     calc_terr_fields = {
#         'B_STATE': ogr.OFTString,
#         'B_COUNTY': ogr.OFTString,
#         'B_FIPS': ogr.OFTString,
#         CENTROID_FIELD: ogr.OFTString
#         }
#     simplify_merge_polygon_shapefiles(in_terr_filenames, calc_terr_fields, 
#                                       out_terr_filename)
    
    # Marine boundaries
    eez_orig_sfname = 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp'
    grid_sfname = 'world_grid_2.5.shp'
    # Same as ANCILLARY_FILES['marine']['file']
    eez_outfname = 'eez_gridded_boundaries_2.5.shp'
#     grid_sfname = 'world_grid_5.shp'
#     eez_outfname = 'eez_gridded_boundaries_5.shp'
#     intersect_outfname = 'gridded_eez.shp'
    
    orig_eez_filename = os.path.join(pth, eez_orig_sfname)
    grid_shp_filename = os.path.join(pth, grid_sfname)
    intersect_filename = os.path.join(pth, eez_outfname)
    
    
#     in_eez_filenames = [intersect_outfname]
#     out_eez_filename = os.path.join(pth, eez_outfname)
    calc_eez_fields = {CENTROID_FIELD: ogr.OFTString}
#     simplify_merge_polygon_shapefiles(in_eez_filenames, calc_eez_fields, 
#                                       out_eez_filename)
    
    intersect_polygon_with_grid(orig_eez_filename, grid_shp_filename, 
                                calc_eez_fields, intersect_filename)
    
"""
pth = '/tank/data/bison/2019/ancillary'
grid_sfname = 'world_grid_5.shp'
shp_filename = os.path.join(pth, grid_sfname)


pth, basename = os.path.split(shp_filename)
idxname, _ = os.path.splitext(basename)
idx_filename = os.path.join(pth, idxname)

prop = rtree.index.Property()
prop.set_filename(idx_filename)

driver = ogr.GetDriverByName("ESRI Shapefile")
datasrc = driver.Open(shp_filename, 0)
lyr = datasrc.GetLayer()
spindex = rtree.index.Index(idxname, interleaved=False, properties=prop)

for fid in range(0, lyr.GetFeatureCount()):
    feat = lyr.GetFeature(fid)
    geom = feat.geometry()
    wkt = geom.ExportToWkt()
    # OGR returns xmin, xmax, ymin, ymax
    xmin, xmax, ymin, ymax = geom.GetEnvelope()
    # Rtree takes xmin, xmax, ymin, ymax IFF interleaved = False
    spindex.insert(fid, (xmin, xmax, ymin, ymax), obj=wkt)

spindex.close()

"""