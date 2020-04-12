"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

             Lifemapper Project, lifemapper [at] ku [dot] edu, 
             Biodiversity Institute,
             1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
             This program is free software; you can redistribute it and/or modify 
             it under the terms of the GNU General Public License as published by 
             the Free Software Foundation; either version 2 of the License, or (at 
             your option) any later version.
  
             This program is distributed in the hope that it will be useful, but 
             WITHOUT ANY WARRANTY; without even the implied warranty of 
             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
             General Public License for more details.
  
             You should have received a copy of the GNU General Public License 
             along with this program; if not, write to the Free Software 
             Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
             02110-1301, USA.
"""
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import subprocess
import time

from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, ANCILLARY_DIR, ANCILLARY_FILES, ProviderActions)
from common.intersect_one import intersect_csv_and_shapefiles
from common.tools import getLogger

from gbif.gbifmod import GBIFReader

from provider.providermod import BisonMerger

# .............................................................................
def get_line_count(filename):
    """ find total number lines in a file """
    cmd = "wc -l {}".format(filename)
    info, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    temp = info.split(b'\n')[0]
    line_count = int(temp.split()[0])
    return line_count

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
def _get_process_count():
    return cpu_count() - 2

# .............................................................................
def _find_chunk_files(big_csv_filename):
    """ Finds multiple smaller input csv files from a large input csv file, 
    if they exist, and return these filenames, paired with output filenames 
    for the results of processing these files. """
    cpus2use = _get_process_count()
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    pth, basename = os.path.split(in_base_filename)
    # We know filename starts with 'step' followed by 1 char integer
    nextstep = str(int(basename[4]) + 1)
    out_base_filename = os.path.join(pth, basename[:4] + nextstep + basename[5:])
    total_lines = get_line_count(big_csv_filename) - 1
    chunk_size = int(total_lines / cpus2use)
    
    csv_filename_pairs = []
    start = 1
    stop = chunk_size
    while start <= total_lines:
        in_filename = '{}_{}-{}{}'.format(in_base_filename, start, stop, ext)
        out_filename =  '{}_{}-{}{}'.format(out_base_filename, start, stop, ext)
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
def get_chunk_files(big_csv_filename):
    """ Creates multiple smaller input csv files from a large input csv file, and 
    return these filenames, paired with output filenames for the results of 
    processing these files. """
    csv_filename_pairs, chunk_size = _find_chunk_files(big_csv_filename)
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
            in_filename = '{}_{}-{}.csv'.format(in_base_filename, start, stop)
            out_filename =  '{}_{}-{}.csv'.format(out_base_filename, start, stop)
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
def step_parallel(in_csv_filename, terrestrial_data, marine_data, ancillary_path,
                  out_csv_filename):
    """Main method for parallel execution of geo-referencing script"""
    csv_filename_pairs, header = get_chunk_files(in_csv_filename)
    
#     in_csv_fn, out_csv_fn = csv_filename_pairs[0]
#     in_csv_fn = '/tank/data/bison/2019/Terr/tmp/step3_occurrence_lines_30000001-40000001_8000001-9000000.csv'
#     out_csv_fn = '/tank/data/bison/2019/Terr/tmp/step4_occurrence_lines_30000001-40000001_8000001-9000000.csv'
#     intersect_csv_and_shapefiles(in_csv_fn, terrestrial_data, 
#                 marine_data, ancillary_path, out_csv_fn)
 
    with ProcessPoolExecutor() as executor:
        for in_csv_fn, out_csv_fn in csv_filename_pairs:
            executor.submit(
                intersect_csv_and_shapefiles, in_csv_fn, terrestrial_data, 
                marine_data, ancillary_path, out_csv_fn)
    
    try:
        outf = open(out_csv_filename, 'w', encoding='utf-8')
        outf.write('{}'.format(header))
        for _, small_csv_fn in csv_filename_pairs:
            lineno = 0
            try:
                for line in open(small_csv_fn, 'r', encoding='utf-8'):
                    # Skip header in each file
                    if lineno == 0:
                        pass
                    else:
                        outf.write('{}'.format(line))
                    lineno += 1
            except Exception as inner_err:
                print('Failed to write {} to merged file; {}'.format(small_csv_fn, inner_err))
    except Exception as outer_err:
        print('Failed to write to {}; {}'.format(out_csv_filename, outer_err))
    finally:
        outf.close()
        
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Parse a GBIF occurrence dataset downloaded
                                     from the GBIF occurrence web service in
                                     Darwin Core format into BISON format.  
                                 """))
    parser.add_argument('occ_file_or_path', type=str, 
                        help="""
                        Input GBIF occurrence file for data transform (path
                        also contains metadata)
                        or 
                        directory containing files for BISON provider merge.  
                        
                        If the subdirectories 'tmp' and 'out' 
                        are not present in the same directory as the raw data, 
                        they will be  created for temp and final output files.
                        """)
    parser.add_argument('--step', type=int, default=1, choices=[1,2,3,4,5,10, 13],
                        help="""
                        Step number for data processing:
                           1: Only for GBIF data. 
                              Create lookup tables, transform and fill BISON
                              records from GBIF data and lookup tables:
                              * Resource/Provider lookup
                                * Query GBIF dataset API + datasetKey for 
                                  dataset info for Bison 'resource' fields and 
                                  (publishing)OrganizationKey.
                                * Query GBIF organization API + organizationKey for 
                                  organization info for BISON 'provider' fields'
                              * GBIF record and field filter/transform
                                including Resource and Organization values 
                                from Resource/Provider lookup tables 
                              * Name info (provided_scientific_name, taxonKey)
                                and UUIDs are saved in records for GBIF API 
                                resolution in Step 2.
                           2: Only for GBIF data files.
                              Create name lookup table, update BISON
                              records from name lookup table:
                              * Query GBIF parser + scientificName if available, 
                                or GBIF species API + taxonKey --> name lookup
                              * Fill clean_provided_scientific_name field 
                                with resolved values saved in name lookup table.
                           3: Fill fields:
                              - ITIS fields - resolve with ITIS lookup and
                                clean_provided_scientific_name filled in step2
                              - establishment_means - resolve with establishment
                                means lookup and ITIS TSN or 
                                clean_provided_scientific_name
                              - geo fields - 
                                - lon/lat and centroid for records without
                                  lon/lat and with state+county or fips
                                  from terrestrial centroid coordinates
                           4: Fill fields:
                              - geo fields: calculate point-in-polygon for 
                                records with new or existing lon/lat 
                                - resolve 1st with terrestrial 
                                  shapefile US_CA_Counties_Centroids, filling 
                                  calculated_state, calculated_county, 
                                  calculated_fips
                                - or resolve second with marine shapefile 
                                  World_EEZ_v8_20140228_splitpolygons
                                  filling calculated_waterbody and mrgid - only 
                                  if terrestrial georef returns 0 or > 1 result
                           5: Only for BISON provider data. 
                              Transform and fill BISON ...
                           10: Test data:
                              - ITIS fields - resolve with ITIS lookup and
                        """)
    args = parser.parse_args()
    occ_file_or_path = args.occ_file_or_path
    step = args.step
    
    overwrite = True
    tmpdir = 'tmp'
    outdir = 'out'

    inpath = occ_file_or_path
    logbasename = 'step{}'.format(step)
    if step != 5:
        occ_fname = occ_file_or_path
        inpath, basefname_wext = os.path.split(occ_file_or_path)
        basefname, ext = os.path.splitext(basefname_wext)
        logbasename = '{}_{}'.format(logbasename, basefname)

    datapth, _ = os.path.split(inpath)
    tmppath = os.path.join(inpath, tmpdir)
    outpath = os.path.join(inpath, outdir)
    os.makedirs(tmppath, mode=0o775, exist_ok=True)
    os.makedirs(outpath, mode=0o775, exist_ok=True)

    # ancillary data for record update    
    ancillary_path = os.path.join(datapth, ANCILLARY_DIR)
    terrestrial_shpname = os.path.join(
        ancillary_path, ANCILLARY_FILES['terrestrial']['file'])
    estmeans_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['establishment_means']['file'])
    marine_shpname = os.path.join(ancillary_path, 
                                  ANCILLARY_FILES['marine']['file'])
    itis2_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['itis']['file'])
    resource_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['resource']['file'])
    provider_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['provider']['file'])
        
    # Files of name lookup and list for creation 
    nametaxa_fname = os.path.join(tmppath, 'step1_{}_sciname_taxkey_list.csv'.format(basefname))
    canonical_lut_fname = os.path.join(tmppath, 'step2_{}_canonical_lut.csv'.format(basefname))        
    # Output CSV files of all records after initial creation or field replacements
    pass1_fname = os.path.join(tmppath, 'step1_{}.csv'.format(basefname))
    pass2_fname = os.path.join(tmppath, 'step2_{}.csv'.format(basefname))
    pass3_fname = os.path.join(tmppath, 'step3_{}.csv'.format(basefname))
    pass4_fname = os.path.join(tmppath, 'step4_{}.csv'.format(basefname))    
    # OUTPUT filenames for merged dataset/resource and organization/provider lookups
    merged_dataset_lut_fname = os.path.join(tmppath, 'merged_dataset_lut.csv')
    merged_org_lut_fname = os.path.join(tmppath, 'merged_organization_lut.csv')
    
    if not os.path.exists(occ_file_or_path):
        raise Exception('File or path {} does not exist'.format(occ_file_or_path))
    else:
        start_time = time.time()
        logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
        if step == 1:
            logger = getLogger(logbasename, logfname)
            gr = GBIFReader(inpath, tmpdir, outdir, logger)
            gr.write_dataset_org_lookup(
                merged_dataset_lut_fname, resource_lut_fname, 
                merged_org_lut_fname, provider_lut_fname, 
                outdelimiter=BISON_DELIMITER)
            # Pass 1 of CSV transform, initial pull, standardize, 
            gr.transform_gbif_to_bison(
                occ_file_or_path, 
                merged_dataset_lut_fname, 
                merged_org_lut_fname, 
                nametaxa_fname, pass1_fname)
            logger.info('Elapsed time {} for step {}, file {}'.format(
                time.time() - start_time, step, occ_file_or_path))
            
        elif step == 2:
            logger = getLogger(logbasename, logfname)
            gr = GBIFReader(inpath, tmpdir, outdir, logger)
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(nametaxa_fname):
                gr.gather_name_input(pass1_fname, nametaxa_fname)
                
            canonical_lut = gr.get_canonical_lookup(nametaxa_fname, 
                                                    canonical_lut_fname)
            # Pass 2 of CSV transform
            gr.update_bison_names(pass1_fname, pass2_fname, canonical_lut)
            logger.info('Elapsed time {} for step {}, file {}'.format(
                time.time() - start_time, step, occ_file_or_path))
            
        elif step == 3:
            logger = getLogger(logbasename, logfname)
            bf = BisonFiller(pass2_fname, log=logger)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.update_itis_estmeans_centroid(itis2_lut_fname, estmeans_fname, 
                                             terrestrial_shpname, pass3_fname, 
                                             fromGbif=True)
            logger.info('Elapsed time {} for step {}, file {}'.format(
                time.time() - start_time, step, occ_file_or_path))

        elif step == 4:
            terr_data = ANCILLARY_FILES['terrestrial']
            marine_data = ANCILLARY_FILES['marine']
            step_parallel(pass3_fname, terr_data, marine_data, ancillary_path,
                          pass4_fname)
            
            print('Elapsed time {} for step {}, file {}'.format(
                time.time() - start_time, step, occ_file_or_path))
            
        elif step == 5:
            logger = getLogger(logbasename, logfname)
            merger = BisonMerger(outpath, logger)
            old_resources = merger.read_old_resources(resource_lut_fname)
            provider_datasets = merger.assemble_files(inpath, old_resources)
            
            for resource_ident, pvals in provider_datasets.items():
                fname = pvals['filename']
                action = pvals['action']
                if action in (ProviderActions.wait, ProviderActions.unknown):
                    print('Wait to process {}'.format(resource_ident))
                else:
                    if fname is None:
                        fname = 'bison_{}.csv'.format(resource_ident)
                    infile = os.path.join(inpath, fname)
                    if not os.path.exists(infile):
                        raise Exception('File {} does not exist for {}'
                                        .format(fname, resource_ident))
                    basename, _ = os.path.splitext(fname)
                    outfile1 = os.path.join(tmppath, basename + '_clean.csv')
                    outfile2 = os.path.join(tmppath, basename + '_958itis_em_geo.csv')            
                    outfile3 = os.path.join(outpath, basename + '_final.csv')
                    if os.path.exists(outfile3):
                        print('Final file {} already exists for {}'
                              .format(fname, resource_ident))
                    else:
                        # Step 1: rewrite filling ticket/constant vals for resource/provider
                        merger.rewrite_bison_data(infile, outfile1, old_resources, 
                                                  resource_ident, pvals['resource'], 
                                                  pvals['resource_url'], action)
                        # Step 2: rewrite filling lookup vals from 
                        # itis, establishment_means and centroid 
                        bf = BisonFiller(outfile1, log=logger)
                        bf.update_itis_estmeans_centroid(itis2_lut_fname, estmeans_fname, 
                                                         terrestrial_shpname, outfile2, 
                                                         fromGbif=False)
                        # Step 3: of CSV transform
                        # Use Derek D. generated ITIS lookup itis2_lut_fname
                        bf = BisonFiller(outfile2, log=logger)
                        bf.update_point_in_polygons(ancillary_path, outfile3)

        elif step == 10:
            logger = getLogger(logbasename, logfname)
            bf = BisonFiller(pass3_fname, log=logger)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.test_bison_outfile(fromGbif=True)
        elif step == 13:
            logger = getLogger(logbasename, logfname)
            bf = BisonFiller(pass3_fname, log=logger)
            # Pass 4 of CSV transform, final step, point-in-polygon intersection
            ((terr_data_src, terrlyr, terrindex, terrfeats, terr_bison_fldnames), 
             (eez_data_src, eezlyr, marindex, marfeats, 
              mar_bison_fldnames)) = bf.test_point_in_polygons(
                  ancillary_path, pass4_fname)
"""

# /tank/data/bison/2019/Terr/occurrence_lines_5000-10000.csv --step=4

import os
from common.bisonfill import BisonFiller
from common.constants import (BISON_DELIMITER, ANCILLARY_DIR, ANCILLARY_FILES, 
                              ProviderActions)
from common.tools import getLogger
from gbif.gbifmod import GBIFReader
from provider.providermod import BisonMerger

occ_file_or_path = '/tank/data/bison/2019/Terr/occurrence_lines_30000001-40000001.csv'
occ_file_or_path = '/tank/data/bison/2019/Terr/occurrence_lines_5000-10000.csv'
step = 4

inpath, basefname_wext = os.path.split(occ_file_or_path)
basefname, ext = os.path.splitext(basefname_wext)
datapth, _ = os.path.split(inpath)
tmppath = os.path.join(inpath, 'tmp')
ancillary_path = os.path.join(datapth, ANCILLARY_DIR)
terrestrial_shpname = os.path.join(
    ancillary_path, ANCILLARY_FILES['terrestrial']['file'])
estmeans_fname = os.path.join( 
    ancillary_path, ANCILLARY_FILES['establishment_means']['file'])
itis2_lut_fname = os.path.join( 
    ancillary_path, ANCILLARY_FILES['itis']['file'])

pass2_fname = os.path.join(tmppath, 'step2_{}.csv'.format(basefname))
pass3_fname = os.path.join(tmppath, 'step3_{}.csv'.format(basefname))
pass4_fname = os.path.join(tmppath, 'step4_{}.csv'.format(basefname))    
_
logbasename = 'step{}_{}'.format(step, basefname)
logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
logger = getLogger(logbasename, logfname)

# TEST step 13
self = BisonFiller(pass3_fname, log=logger)
# TEST - go to BisonFiller code




# step 3
bf = BisonFiller(pass2_fname, log=logger)
bf.update_itis_estmeans_centroid(itis2_lut_fname, estmeans_fname, 
                                             terrestrial_shpname, pass3_fname, 
                                             fromGbif=True)

# step 4
bf = BisonFiller(pass3_fname, log=logger)
bf.update_point_in_polygons(ancillary_path, pass4_fname)

"""
