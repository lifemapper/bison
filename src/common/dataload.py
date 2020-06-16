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
import time

from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, ProviderActions, LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, 
    OUTPUT_DIR, PROVIDER_DELIMITER)
from common.inputdata import ANCILLARY_FILES
from common.intersect_one import intersect_csv_and_shapefiles
from common.tools import get_logger, get_line_count, get_header

from gbif.gbifmod import GBIFReader

from provider.providermod import BisonMerger

# .............................................................................
def _get_process_count():
    return cpu_count() - 2

# .............................................................................
def _find_chunk_files(big_csv_filename, out_csv_filename):
    """ Finds multiple smaller input csv files from a large input csv file, 
    if they exist, and return these filenames, paired with output filenames 
    for the results of processing these files. """
    cpus2use = _get_process_count()
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
def step_parallel(in_csv_filename, terrestrial_data, marine_data, ancillary_path,
                  out_csv_filename, from_gbif=True):
    """Main method for parallel execution of geo-referencing script"""
    csv_filename_pairs, header = get_chunk_files(
         in_csv_filename, out_csv_filename=out_csv_filename)
    
#     in_csv_fn, out_csv_fn = csv_filename_pairs[0]
#     intersect_csv_and_shapefiles(in_csv_fn, terrestrial_data, 
#                 marine_data, ancillary_path, out_csv_fn, False)
 
    with ProcessPoolExecutor() as executor:
        for in_csv_fn, out_csv_fn in csv_filename_pairs:
            executor.submit(
                intersect_csv_and_shapefiles, in_csv_fn, terrestrial_data, 
                marine_data, ancillary_path, out_csv_fn, from_gbif)
    
    try:
        outf = open(out_csv_filename, 'w', encoding='utf-8')
        outf.write('{}'.format(header))
        smfile_linecount = 0
        for _, small_csv_fn in csv_filename_pairs:
            curr_linecount = get_line_count(small_csv_fn) - 1
            print('Appending {} records from {}'.format(
                curr_linecount, small_csv_fn))
            # Do not count header
            smfile_linecount += (curr_linecount)
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
    
    lgfile_linecount = get_line_count(out_csv_filename) - 1
    print('Total {} of {} records written to {}'.format(
        lgfile_linecount, smfile_linecount, out_csv_filename))
    
# .............................................................................
def do_track_providers(step, basefname, outpath):
    resource_count_fname = os.path.join(
        outpath,  '{}.count.resource.txt'.format(basefname))
    provider_count_fname = os.path.join(
        outpath,  '{}.count.provider.txt'.format(basefname))
    # If both exist, do nothing
    if (os.path.exists(resource_count_fname) and 
        os.path.exists(provider_count_fname)):
        print('Files {} and {} exist'.format(
            resource_count_fname, provider_count_fname))
        track_providers = False
    else:
        track_providers = True
    # If just starting or only one exists, delete both
    if step == 1 or track_providers:
        if os.path.exists(resource_count_fname):
            print('Deleting {}'.format(resource_count_fname))
            os.remove(resource_count_fname)
        if os.path.exists(provider_count_fname):
            print('Deleting {}'.format(provider_count_fname))
            os.remove(provider_count_fname)
    return resource_count_fname, provider_count_fname, track_providers
    
# .............................................................................
stephelp="""
    Step number for data processing:
       1: Only for GBIF data files. 
          Create lookup tables, transform and fill BISON records from GBIF data 
          and lookup tables:
          * Resource/Provider lookup
            * Query GBIF dataset API + datasetKey for dataset info for Bison 
              'resource' fields and (publishing)OrganizationKey.
            * Query GBIF organization API + organizationKey for organization  
              info for BISON 'provider' fields'
          * GBIF record and field filter/transform including Resource and 
            Organization values from Resource/Provider lookup tables 
          * Name info (provided_scientific_name, taxonKey) and UUIDs are saved 
            in records for GBIF API resolution in Step 2.
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
          Process BISON provider data to:
          * Assemble datasets, rewriting datasets to 
            add, retain, rename
          * Fill ITIS, establishment means, centroid fields 
          * Fill geo-political boundary fields 
            - identify fips/county/state or marine EEZ 
              encompassing the point
       10: Test data:
          - ITIS fields - resolve with ITIS lookup and
    """        
# ...............................................
if __name__ == '__main__':
    """
    Expected directory structure and file locations:
    <basepath>  (ex: /tank/data/bison/2019)
        ancillary
        <gbif workpath>: contains input data [ CA_USTerr_gbif | US_gbif | provider ]
            tmp: contains partially processed files and logs
                s1-* | s2-* | s3-* | s4-* directories
                    chunk: subsets of large files for parallel processing
            output: output data
        <provider workpath>: contains input data [ provider ]
            tmp: contains partially processed files and logs
                s1-* | s2-* | s3-* directories
                    chunk: subsets of large files for parallel processing
            output: output data
    """
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Process a GBIF occurrence dataset downloaded
                from the GBIF occurrence web service in Darwin Core format 
                or multiple BISON provider datasets."""))
    parser.add_argument('data_source', type=str, default='gbif', 
                        choices=['gbif', 'provider'],
                        help="""
                        Is input a single GBIF occurrence file or directory of 
                        data provider files """)
    parser.add_argument('input_data', type=str, 
                        help="""
                        Full path and filename of GBIF occurrence file or 
                        directory containing files for BISON provider merge.  
                        """)
    parser.add_argument(
        '--step', type=int, default=1, choices=[1,2,3,4,99], help=stephelp)
    args = parser.parse_args()
    data_source = args.data_source
    input_data = args.input_data
    step = args.step
    
    if data_source == 'gbif':
        if not os.path.exists(input_data):
            raise Exception('Input file {} does not exist'.format(input_data))
        workpath, basefname_wext = os.path.split(input_data)
    else:
        workpath = input_data
        
    basepath, _ = os.path.split(workpath)
    ancillary_path = os.path.join(basepath, ANCILLARY_DIR)

    overwrite = True
    tmppath = os.path.join(workpath, TEMP_DIR)
    outpath = os.path.join(workpath, OUTPUT_DIR)
    os.makedirs(tmppath, mode=0o775, exist_ok=True)
    os.makedirs(outpath, mode=0o775, exist_ok=True)

    # ancillary data for record update    
    terrestrial_shpname = os.path.join(
        ancillary_path, ANCILLARY_FILES['terrestrial']['file'])
    estmeans_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['establishment_means']['file'])
    marine_shpname = os.path.join(ancillary_path, 
                                  ANCILLARY_FILES['marine']['file'])
    terr_data = ANCILLARY_FILES['terrestrial']
    marine_data = ANCILLARY_FILES['marine']
    itis2_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['itis']['file'])
    resource_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['resource']['file'])
    provider_lut_fname = os.path.join(
        ancillary_path, ANCILLARY_FILES['provider']['file'])
    merged_dataset_lut_fname = os.path.join(ancillary_path, 'merged_dataset_lut.csv')
    merged_org_lut_fname = os.path.join(ancillary_path, 'merged_organization_lut.csv')

    # For GBIF processing, steps 1-4, files of name lookup and list for creation 
    if data_source == 'gbif':
        basefname, ext = os.path.splitext(basefname_wext)
        logbasename = 'step{}-{}'.format(step, basefname)
        logfname = os.path.join(tmppath, '{}.log'.format(logbasename))

        nametaxa_fname = os.path.join(tmppath, 'step1_{}_sciname_taxkey_list.csv'
                                      .format(basefname))
        canonical_lut_fname = os.path.join(tmppath, 'step2_{}_canonical_lut.csv'
                                           .format(basefname))        
        
        s1dir = os.path.join(tmppath, 's1-gbif2bison')
        s2dir = os.path.join(tmppath, 's2-scinames')
        s3dir = os.path.join(tmppath, 's3-itisescent')
        s4dir = os.path.join(tmppath, 's4-geo')
        fixdir = os.path.join(tmppath, 's99-fix')
        for sdir in (s1dir, s2dir, s3dir, s4dir, fixdir):
            os.makedirs(sdir, mode=0o775, exist_ok=True)
        # Output CSV files of all records after initial creation or field replacements
        pass1_fname = os.path.join(s1dir, '{}.csv'.format(basefname))
        pass2_fname = os.path.join(s2dir, '{}.csv'.format(basefname))
        pass3_fname = os.path.join(s3dir, '{}.csv'.format(basefname))
        pass4_fname = os.path.join(s4dir, '{}.csv'.format(basefname))
        
        (resource_count_fname, provider_count_fname, 
         track_providers) = do_track_providers(step, basefname, outpath)
        
        start_time = time.time()
        # ..........................................................
        # Step 1: assemble metadata, initial rewrite to BISON format, (GBIF-only)
        # fill resource/provider, check coords (like provider step 1)
        if step == 1:
            logger = get_logger(logbasename, logfname)
            gr = GBIFReader(workpath, logger)
            # Merge existing provider and resource metadata with GBIF dataset 
            # metadata files and GBIF API-returned metadata
            gr.write_dataset_org_lookup(
                merged_dataset_lut_fname, resource_lut_fname, 
                merged_org_lut_fname, provider_lut_fname, 
                outdelimiter=BISON_DELIMITER)
            # initial conversion of GBIF to BISON fields and standardization
            # Discard records from BISON org, bison IPT
            gr.transform_gbif_to_bison(
                input_data, 
                merged_dataset_lut_fname, 
                merged_org_lut_fname, 
                nametaxa_fname, pass1_fname)
            
            gr.write_resource_provider_stats(
                resource_count_fname, provider_count_fname)
        # ..........................................................
        # Step 2: assemble and parse names, rewrite records with clean names
        elif step == 2:
            logger = get_logger(logbasename, logfname)
            gr = GBIFReader(workpath, logger)
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(nametaxa_fname):
                gr.gather_name_input(pass1_fname, nametaxa_fname)
                
            canonical_lut = gr.get_canonical_lookup(nametaxa_fname, 
                                                    canonical_lut_fname)
            # Discard records with no clean name
            gr.update_bison_names(
                pass1_fname, pass2_fname, canonical_lut, 
                track_providers=track_providers)
            
            gr.write_resource_provider_stats(
                resource_count_fname, provider_count_fname)
        # ..........................................................
        # Step 3: rewrite records filling ITIS fields, establishment_means, and 
        # county centroids for records without coordinates 
        elif step == 3:
            logger = get_logger(logbasename, logfname)
            bf = BisonFiller(logger)
            # No discards
            bf.update_itis_estmeans_centroid(
                itis2_lut_fname, estmeans_fname, terrestrial_shpname, 
                pass2_fname, pass3_fname, from_gbif=True,
                track_providers=track_providers)
            if track_providers:
                bf.write_resource_provider_stats(
                    resource_count_fname, provider_count_fname)
        # ..........................................................
        # Step 4: split into smaller files, parallel process 
        # Identify enclosing terrestrial or marine polygons, rewrite with 
        # calculated state/county/fips or mrgid/eez 
        elif step == 4:
            # No discards
            step_parallel(pass3_fname, terr_data, marine_data, ancillary_path,
                          pass4_fname, from_gbif=True)
            if track_providers:
                logger = get_logger(logbasename, logfname)
                gr = GBIFReader(workpath, logger)
                gr.count_provider_resource(pass4_fname)
                gr.write_resource_provider_stats(
                    resource_count_fname, provider_count_fname)
            pass

        # ..........................................................
        # Step 99: fix something
        elif step == 99:
            infile = os.path.join(s4dir, 'occurrence_lines_1-2000.csv')
            fixfile = os.path.join(fixdir, 'occurrence_lines_1-2000.csv')
            logger = get_logger(logbasename, logfname)
            bf = BisonFiller(logger)
            bf.rewrite_recs(infile, fixfile, BISON_DELIMITER)

        else:
            print('No step {} for data_source {}'.format(step, data_source))
            
        # Log time elapsed for steps 1-4 
        minutes = (time.time() - start_time) / 60
        try:
            logger.info('Elapsed minutes {} for step {}, file {}'.format(
                minutes, step, input_data))
        except:
            print('Elapsed minutes {} for step {}, file {}'.format(
                minutes, step, input_data))
            
            
    elif data_source == 'provider':
        combo_logbasename = 'step{}-provider'.format(step)
        combo_logfname = os.path.join(tmppath, '{}.log'.format(combo_logbasename))
        combo_logger = get_logger(combo_logbasename, combo_logfname)
        merger = BisonMerger(combo_logger)
        old_resources = merger.read_resources(merged_dataset_lut_fname)
        # Pull metadata from ticket first, 
        # if missing, fill with old provider table data second
        prov_dataload_metadata = merger.assemble_files(workpath, old_resources)
#         fixme = ['440,100045', '440,100061', 'emammal', 'nplichens']
#         fixme = ['440,100004']
        for resource_ident, pvals in prov_dataload_metadata.items():
            ignore_me = False
            fname = pvals['filename']
            action = pvals['action']
            resource_url = pvals['resource_url']
            resource_name = pvals['resource']
            
            if action in (ProviderActions.wait, ProviderActions.unknown):
                combo_logger.info('Wait to process {}'.format(resource_ident))
            else:
                if fname is None:
                    fname = 'bison_{}.csv'.format(resource_ident)
                    outfname = fname
                else:
                    # New input files have .txt extension
                    outfname = '{}.csv'.format(os.path.splitext(fname)[0])
                combo_logger.info('{}: {} {}'.format(
                    resource_ident, action, fname))
                infile = os.path.join(workpath, fname)
                if not os.path.exists(infile):
                    raise Exception('File {} does not exist for {}'
                                    .format(fname, resource_ident))
                s1dir = os.path.join(tmppath, 's1-clean')
                # No step 2 for BISON provider data
                s3dir = os.path.join(tmppath, 's3-itisescent')
                s4dir = os.path.join(tmppath, 's4-geo')
                for sdir in (s1dir, s2dir, s3dir):
                    os.makedirs(sdir, mode=0o775, exist_ok=True)
                outfile1 = os.path.join(s1dir, outfname)
                outfile3 = os.path.join(s3dir, outfname)            
                outfile4 = os.path.join(s4dir, outfname)
                logbasename = '{}.step{}'.format(outfname, step)                
            # ..........................................................
            # Step 1: rewrite, handle quotes, fill ticket/constant vals for 
            # resource/provider
            # Same process as GBIF Step 1, different resource/provider inputs
            if step == 1 and not ignore_me:
                if os.path.exists(outfile1):
                    combo_logger.info('  Existing step 1 output in {}'.format(
                        outfile1))
                else:
                    combo_logger.info('  Process step 1 output to {}'.format(
                        outfile1))
                    merger.reset_logger(s1dir, logbasename)
                    in_delimiter = PROVIDER_DELIMITER
                    if action in (ProviderActions.rename, ProviderActions.rewrite):
                        in_delimiter = BISON_DELIMITER
                    merger.rewrite_bison_data(
                        infile, outfile1, resource_ident, resource_name, 
                        resource_url, action, in_delimiter)
            # ..........................................................
            # Step 2: Nope
            elif step == 2:
                combo_logger.info('Step 2 is only valid for GBIF data')
            # ..........................................................
            # Step 3: fill lookup vals from ITIS, establishment_means and county centroid files
            # Identical to GBIF Step 3
            elif step == 3 and not ignore_me:
                if os.path.exists(outfile3):
                    combo_logger.info('  Existing step 3 output in {}'.format(
                        outfile3))
                else:
                    combo_logger.info('  Process step 3 output to {}'.format(
                        outfile3))
                    logfname = os.path.join(s2dir, '{}.log'.format(logbasename))
                    logger = get_logger(logbasename, logfname)
                    bfiller = BisonFiller(logger)
                    bfiller.update_itis_estmeans_centroid(
                        itis2_lut_fname, estmeans_fname, terrestrial_shpname, 
                        outfile1, outfile3, from_gbif=False)
            # ..........................................................
            # Step 4: split and process large files in parallel
            # Identical to GBIF Step 4
            elif step == 4 and not ignore_me:
                if os.path.exists(outfile4):
                    combo_logger.info('  Existing step 4 output in {}'.format(
                        outfile4))
                else:
                    lcount = get_line_count(outfile3)
                    if lcount < LOGINTERVAL:
                        combo_logger.info('  Process step 4 output to {}'.format(
                            outfile4))
                        logfname = os.path.join(s3dir, '{}.log'.format(logbasename))
                        logger = get_logger(logbasename, logfname)
                        bfiller = BisonFiller(logger)
                        bfiller.update_point_in_polygons(
                            terr_data, marine_data, ancillary_path, outfile3, outfile4)
                    else:
                        combo_logger.info('  Parallel Process step 4 ({} lines) output to {}'
                                    .format(lcount, outfile4))
                        # Create chunk input files for parallel processing
                        csv_filename_pairs, header = get_chunk_files(
                             outfile3, out_csv_filename=outfile4)
                        step_parallel(outfile3, terr_data, marine_data, ancillary_path,
                                      outfile4, from_gbif=False)
            # ..........................................................
            # Step 99: fix something
            elif step == 99 and not ignore_me:
                fixdir = os.path.join(tmppath, 's99-fix')
                os.makedirs(fixdir, mode=0o775, exist_ok=True)
                outfile99 = os.path.join(fixdir, outfname)
                if not os.path.exists(outfile99):
                    combo_logger.info('  Re-write step 3 output with fix to {}'.format(
                        outfile99))
                    merger.reset_logger(fixdir, logbasename)
                    merger.rewrite_bison_data(
                        outfile4, outfile99, resource_ident, resource_name, 
                        resource_url, action, BISON_DELIMITER)
                            
            else:
                print('Ignore {} {}'.format(resource_ident, fname))

        
"""


import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import time

from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, ProviderActions, LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, 
    OUTPUT_DIR, PROVIDER_DELIMITER)
from common.inputdata import ANCILLARY_FILES
from common.intersect_one import intersect_csv_and_shapefiles
from common.tools import get_logger, get_line_count, get_header

from gbif.gbifmod import GBIFReader

from provider.providermod import BisonMerger

from common.dataload import *


data_source = 'gbif'
input_data = '/tank/data/bison/2019/CA_USTerr_gbif/occurrence_lines_10000001-20000001.csv'
input_data = '/tank/data/bison/2019/CA_USTerr_gbif/occurrence_lines_20000001-30000001.csv'
step = 1

# if data_source == 'gbif':
workpath, basefname_wext = os.path.split(input_data)
#     workpath = input_data
    
basepath, _ = os.path.split(workpath)
ancillary_path = os.path.join(basepath, ANCILLARY_DIR)

overwrite = True
tmppath = os.path.join(workpath, TEMP_DIR)
outpath = os.path.join(workpath, OUTPUT_DIR)

terrestrial_shpname = os.path.join(
    ancillary_path, ANCILLARY_FILES['terrestrial']['file'])

estmeans_fname = os.path.join(
    ancillary_path, ANCILLARY_FILES['establishment_means']['file'])

marine_shpname = os.path.join(ancillary_path, 
                              ANCILLARY_FILES['marine']['file'])

terr_data = ANCILLARY_FILES['terrestrial']
marine_data = ANCILLARY_FILES['marine']
itis2_lut_fname = os.path.join(
    ancillary_path, ANCILLARY_FILES['itis']['file'])

resource_lut_fname = os.path.join(
    ancillary_path, ANCILLARY_FILES['resource']['file'])

provider_lut_fname = os.path.join(
    ancillary_path, ANCILLARY_FILES['provider']['file'])

merged_dataset_lut_fname = os.path.join(ancillary_path, 'merged_dataset_lut.csv')
merged_org_lut_fname = os.path.join(ancillary_path, 'merged_organization_lut.csv')

# if data_source == 'gbif':
basefname, ext = os.path.splitext(basefname_wext)
logbasename = 'step{}-{}'.format(step, basefname)
logfname = os.path.join(tmppath, '{}.log'.format(logbasename))

nametaxa_fname = os.path.join(tmppath, 'step1_{}_sciname_taxkey_list.csv'
                              .format(basefname))

canonical_lut_fname = os.path.join(tmppath, 'step2_{}_canonical_lut.csv'
                                   .format(basefname))        

s1dir = os.path.join(tmppath, 's1-gbif2bison')
s2dir = os.path.join(tmppath, 's2-scinames')
s3dir = os.path.join(tmppath, 's3-itisescent')
s4dir = os.path.join(tmppath, 's4-geo')
fixdir = os.path.join(tmppath, 's99-fix')

pass1_fname = os.path.join(s1dir, '{}.csv'.format(basefname))
pass2_fname = os.path.join(s2dir, '{}.csv'.format(basefname))
pass3_fname = os.path.join(s3dir, '{}.csv'.format(basefname))
pass4_fname = os.path.join(s4dir, '{}.csv'.format(basefname))

(resource_count_fname, provider_count_fname, 
 track_providers) = do_track_providers(step, basefname, outpath)

start_time = time.time()
# # ..........................................................
# # Step 1: assemble metadata, initial rewrite to BISON format

logger = get_logger(logbasename, logfname)
gr = GBIFReader(workpath, logger)

gr.write_dataset_org_lookup(
    merged_dataset_lut_fname, resource_lut_fname, 
    merged_org_lut_fname, provider_lut_fname, 
    outdelimiter=BISON_DELIMITER)

# ..........................................................
# ..........................................................
# gr.transform_gbif_to_bison(
#     input_data, 
#     merged_dataset_lut_fname, 
#     merged_org_lut_fname, 
#     nametaxa_fname, pass1_fname)

(gbif_interp_fname, merged_dataset_lut_fname, merged_org_lut_fname, 
    nametaxa_fname, pass1_fname) = (input_data, merged_dataset_lut_fname, 
    merged_org_lut_fname, nametaxa_fname, pass1_fname)

from common.constants import (BISON_DELIMITER, ENCODING, 
        LOGINTERVAL, PROHIBITED_VALS, LEGACY_ID_DEFAULT, EXTRA_VALS_KEY,
        ALLOWED_TYPE, BISON_ORDERED_DATALOAD_FIELD_TYPE, BISON_IPT_PREFIX, 
        MERGED_RESOURCE_LUT_FIELDS, MERGED_PROVIDER_LUT_FIELDS)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (get_csv_reader, get_csv_dict_reader, get_csv_writer, 
                          open_csv_files, makerow)

from gbif.constants import (GBIF_DELIMITER, TERM_CONVERT, META_FNAME, 
                            BISON_GBIF_MAP, OCC_ID_FLD,
                            CLIP_CHAR, BISON_ORG_UUID, 
                            GBIF_CONVERT_TEMP_FIELD_TYPE, 
                            GBIF_NAMEKEY_TEMP_FIELD, GBIF_NAMEKEY_TEMP_TYPE)
from gbif.gbifmeta import GBIFMetaReader
from gbif.gbifapi import GbifAPI

self = gr
gm_rdr = GBIFMetaReader(self._log)
gbif_header = gm_rdr.get_field_list(self._meta_fname)

self._infields.extend(list(GBIF_CONVERT_TEMP_FIELD_TYPE.keys()))
for fldname, fldmeta in GBIF_CONVERT_TEMP_FIELD_TYPE.items():
    self._fields[fldname] = fldmeta

self._infields.append(GBIF_NAMEKEY_TEMP_FIELD)
self._fields[GBIF_NAMEKEY_TEMP_FIELD] = GBIF_NAMEKEY_TEMP_TYPE

self._outfields.append(GBIF_NAMEKEY_TEMP_FIELD)

dataset_by_uuid = Lookup.initFromFile(merged_dataset_lut_fname, 
    ['gbif_datasetkey', 'dataset_id'], BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
    encoding=ENCODING)

dataset_by_legacyid = Lookup.initFromFile(merged_dataset_lut_fname, 
    ['OriginalResourceID', 'gbif_legacyid'], BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
    encoding=ENCODING)

org_by_uuid = Lookup.initFromFile(merged_org_lut_fname, 
    ['gbif_organizationKey'], BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
    encoding=ENCODING)

org_by_legacyid = Lookup.initFromFile(merged_org_lut_fname, 
    ['OriginalProviderID', 'gbif_legacyid'], BISON_DELIMITER, valtype=VAL_TYPE.DICT, 
    encoding=ENCODING)

if os.path.exists(nametaxa_fname):
    self._log.info('Read name metadata ...')
    nametaxas = Lookup.initFromFile(
        nametaxa_fname, None, BISON_DELIMITER, valtype=VAL_TYPE.SET, 
        encoding=ENCODING)
else:
    nametaxas = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)

recno = 0
dict_reader, inf, writer, outf = open_csv_files(
    gbif_interp_fname, GBIF_DELIMITER, ENCODING, 
    infields=gbif_header, outfname=pass1_fname, 
    outfields=self._outfields, outdelimiter=BISON_DELIMITER)

# for orig_rec in dict_reader:
orig_rec = next(dict_reader)

recno += 1
brec = self._create_rough_bisonrec(orig_rec)
biline = self._complete_bisonrec_pass1(
    brec, dataset_by_uuid, org_by_uuid, org_by_legacyid, 
    nametaxas)

dskey = brec['datasetKey']
dsvals = dataset_by_uuid.lut[dskey]

if biline:
    writer.writerow(biline)
    self._track_provider_resources(brec)

                    
except Exception as e:
self._log.error('Failed on line {}, e = {}'.format(recno, e))
finally:
inf.close()
outf.close()

#         self.write_resource_provider_stats(pass1_fname)
self._log.info('Missing organization ids: {}'.format(self._missing_orgs))    
self._log.info('Missing dataset ids: {}'.format(self._missing_datasets))    

# Write all lookup values
if len(nametaxas.lut) > 0:
nametaxas.write_lookup(
    nametaxa_fname, ['scientificName', 'taxonKeys'], BISON_DELIMITER)            

# ..........................................................

gr.write_resource_provider_stats(
    resource_count_fname, provider_count_fname)


"""
