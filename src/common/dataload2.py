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
    BISON_DELIMITER, LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, 
    OUTPUT_DIR, PROVIDER_DELIMITER, PROVIDER_ACTIONS, ENCODING)
from common.inputdata import ANCILLARY_FILES
from common.intersect_one import intersect_csv_and_shapefiles
from common.lookup import Lookup, VAL_TYPE
from common.tools import get_logger, get_line_count, get_header

from gbif.gbifmod import GBIFReader

from provider.providermod import BisonMerger

# .............................................................................
class DataLoader(object):
    """
    Object for processing a Bison Provider or GBIF CSV files filling ITIS fields, 
    coordinates, terrestrial or marine boundaries, and establishment_means.
    """
    # ...............................................
    def __init__(self, data_source, input_data, step, resource_id=None):
        """
        @summary: Constructor
        """    
        if not os.path.exists(input_data):
            raise Exception('Input file {} does not exist'.format(input_data))
        elif os.path.isdir(input_data):
            workpath = input_data.rstrip(os.path.sep)
            self.rawdata_fname = None
        elif os.path.isfile(input_data):
            self.rawdata_fname = input_data
            workpath, basefname_wext = os.path.split(self.rawdata_fname)
        # filename w/o path or extension
        self.basefname, _ = os.path.splitext(basefname_wext)
            
        self._test_resource_id = resource_id
            
        self.basepath, _ = os.path.split(workpath)
        self.ancillary_path = os.path.join(self.basepath, ANCILLARY_DIR)
        self.tmppath = os.path.join(workpath, TEMP_DIR)
        self.outpath = os.path.join(workpath, OUTPUT_DIR)
        os.makedirs(self.tmppath, mode=0o775, exist_ok=True)
        os.makedirs(self.outpath, mode=0o775, exist_ok=True)
    
        # ancillary data for record update    
        self.terrestrial_shpname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['terrestrial']['file'])
        self.estmeans_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['establishment_means']['file'])
        self.marine_shpname = os.path.join(self.ancillary_path, 
                                      ANCILLARY_FILES['marine']['file'])
        self.itis2_lut_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['itis']['file'])
        
        resource_lut_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['resource']['file'])
        provider_lut_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['provider']['file'])
        self.merged_resource_lut_fname = os.path.join(self.ancillary_path, 'merged_dataset_lut.csv')
        self.merged_provider_lut_fname = os.path.join(self.ancillary_path, 'merged_organization_lut.csv')
        self.resource_count_fname = os.path.join(
            self.outpath,  '{}.count.resource.csv'.format(data_source))
        self.provider_count_fname = os.path.join(
            self.outpath,  '{}.count.provider.csv'.format(data_source))

        self.nametaxa_fname = os.path.join(self.tmppath, 'step1_{}_sciname_taxkey_list.csv'
                                      .format(self.basefname))
        self.canonical_lut_fname = os.path.join(self.tmppath, 'step2_{}_canonical_lut.csv'
                                           .format(self.basefname))        

        self.s1dir = os.path.join(self.tmppath, 's1-rewrite')
        self.s2dir = os.path.join(self.tmppath, 's2-scinames')
        self.s3dir = os.path.join(self.tmppath, 's3-itisescent')
        self.s4dir = os.path.join(self.tmppath, 's4-geo')
        # No step 2 for BISON provider data
        for sdir in (self.s1dir, self.s3dir, self.s4dir):
            os.makedirs(sdir, mode=0o775, exist_ok=True)
        if data_source == 'gbif':
            os.makedirs(self.s2dir, mode=0o775, exist_ok=True)
        
        # All filled in _prep_xxxx function
        self.pass1_fname = None
        self.pass2_fname = None
        self.pass3_fname = None
        self.pass4_fname = None
        self.logger = None
        self.action = None
        self.bisonprov_dataload = None
        self.track_providers = None 
        
        if data_source == 'gbif':
            self._prep_gbif(step, resource_lut_fname, provider_lut_fname)
        elif data_source == 'provider':
            self._prep_bisonprov()
        
    # ...............................................
    def _prep_gbif(self, step, resource_lut_fname, provider_lut_fname):
        self.logger = self._get_step_logger(step)
        self.track_providers = self._do_track_providers()
        self.logger = self._get_step_logger(self.pass1_fname)
        # Output CSV files of all records after initial creation or field replacements
        self.pass1_fname = os.path.join(self.s1dir, '{}.csv'.format(self.basefname))
        self.pass2_fname = os.path.join(self.s2dir, '{}.csv'.format(self.basefname))
        self.pass3_fname = os.path.join(self.s3dir, '{}.csv'.format(self.basefname))
        self.pass4_fname = os.path.join(self.s4dir, '{}.csv'.format(self.basefname))

        if (not os.path.exists(self.merged_provider_lut_fname) or
            not os.path.exists(self.merged_resource_lut_fname)):
            logbasename = 'prep_resource_provider'
            logfname = os.path.join(self.tmppath, '{}.log'.format(logbasename))
            logger = get_logger(logbasename, logfname)
            gr = GBIFReader('/tank/data/bison/2019/CA_USTerr_gbif', logger)
            # Merge existing provider and resource metadata with GBIF dataset 
            # metadata files and GBIF API-returned metadata
            gr.resolve_provider_resource_for_lookup(
                self.merged_resource_lut_fname, resource_lut_fname, 
                self.merged_provider_lut_fname, provider_lut_fname, 
                outdelimiter=BISON_DELIMITER)
        
    # .............................................................................
    def _do_track_providers(self):
        # If both exist, do nothing
        if (os.path.exists(self.resource_count_fname) and 
            os.path.exists(self.provider_count_fname)):
            print('Files {} and {} exist'.format(
                self.resource_count_fname, self.provider_count_fname))
            self.track_providers = False
        else:
            self.track_providers = True
            if os.path.exists(self.resource_count_fname):
                print('Deleting {}'.format(self.resource_count_fname))
                os.remove(self.resource_count_fname)
            if os.path.exists(self.provider_count_fname):
                print('Deleting {}'.format(self.provider_count_fname))
                os.remove(self.provider_count_fname)        
        
    # ...............................................
    def process_gbif_step1(self):
        # Step 1: assemble metadata, initial rewrite to BISON format, (GBIF-only)
        # fill resource/prov0ider, check coords (like provider step 1)
        logger = self._get_step_logger(self.pass1_fname)
        gr = GBIFReader(self.datapath, logger)
        gr.read_resource_provider_stats(
            self.resource_count_fname, self.provider_count_fname)
        # initial conversion of GBIF to BISON fields and standardization
        # Discard records from BISON org, bison IPT
        gr.transform_gbif_to_bison(
            self.rawdata_fname, 
            self.merged_resource_lut_fname, 
            self.merged_provider_lut_fname, 
            self.nametaxa_fname, self.pass1_fname)
        
        gr.write_resource_provider_stats(
            self.resource_count_fname, self.provider_count_fname, overwrite=True)

    # ...............................................
    def process_gbif_step2(self):
        # Step 2: assemble and parse names, rewrite records with clean names
        logger = self._get_step_logger(self.pass2_fname)
        gr = GBIFReader(self.datapath, logger)
        # Reread output ONLY if missing gbif name/taxkey 
        if not os.path.exists(self.nametaxa_fname):
            gr.gather_name_input(self.pass1_fname, self.nametaxa_fname)
            
        canonical_lut = gr.resolve_canonical_taxonkeys_for_lookup(
            self.nametaxa_fname, self.canonical_lut_fname)
        # Discard records with no clean name
        gr.update_bison_names(
            self.pass1_fname, self.pass2_fname, canonical_lut, 
            track_providers=self.track_providers)
            
    # ...............................................
    def process_gbif_step3(self):
        # Step 3: rewrite records filling ITIS fields, establishment_means, and 
        # county centroids for records without coordinates 
        logger = self._get_step_logger(self.pass3_fname)
        bf = BisonFiller(logger)
        # No discards
        bf.update_itis_estmeans_centroid(
            self.itis2_lut_fname, self.estmeans_fname, self.terrestrial_shpname, 
            self.pass2_fname, self.pass3_fname, from_gbif=True,
            track_providers=self.track_providers)

    # ...............................................
    def _get_step_logger(self, step):
        if step == 1:
            stepdir = self.s1dir
        elif step == 2:
            stepdir = self.s2dir
        elif step == 3:
            stepdir = self.s3dir
        elif step == 4:
            stepdir = self.s4dir        
        logfname = os.path.join(stepdir, '{}.log'.format(self.basefname))
        logger = get_logger(self.basefname, logfname)
        return logger

    # ...............................................
    def process_gbif_step4(self):
        # Step 4: split into smaller files, parallel process 
        # Identify enclosing terrestrial or marine polygons, rewrite with 
        # calculated state/county/fips or mrgid/eez 
        terr_data = ANCILLARY_FILES['terrestrial']
        marine_data = ANCILLARY_FILES['marine']
        # No discards
        lcount = get_line_count(self.pass3_fname)
        if lcount < LOGINTERVAL:
            logger = self._get_step_logger(self.pass4_fname)
            bf = BisonFiller(logger)
            logger.info('  Process step 4 output to {}'.format(
                self.pass4_fname))
            bf.update_point_in_polygons(
                self.terr_data, self.marine_data, self.ancillary_path, 
                self.pass3_fname, self.pass4_fname)
        else:
            step_parallel(
                self.pass3_fname, terr_data, marine_data, self.ancillary_path, 
                self.pass4_fname, from_gbif=True)
            

#         # ..........................................................
#         # Step 99: fix something
#         elif step == 99:
#             infile = os.path.join(s4dir, 'occurrence_lines_1-2000.csv')
#             fixfile = os.path.join(fixdir, 'occurrence_lines_1-2000.csv')
#             logfname = os.path.join(fixdir, '{}.log'.format(basefname))
#             logger = get_logger(basefname, logfname)
#             bf = BisonFiller(logger)
#             bf.rewrite_recs(infile, fixfile, BISON_DELIMITER)
#         # ..........................................................
#         # Step 1000: test something
#         elif step == 1000:
#             outfile = os.path.join(outpath, '{}.csv'.format(basefname))
#             if os.path.exists(outfile):
#                 logfname = os.path.join(outpath, '{}.log'.format(basefname))
#                 logger = get_logger(basefname, logfname)
#                 bfiller = BisonFiller(logger)
#                 bfiller.walk_data(
#                     in_fname, terr_data, marine_data, ancillary_path, 
#                     merged_resource_lut_fname, merged_provider_lut_fname)

    # ...............................................
    def _prep_bisonprov(self):
        self.action = None
        bisonprov_lut_fname = os.path.join(
            self.ancillary_path, 'bisonprovider_meta_lut.csv')
        combo_logbasename = 'step{}-provider'.format(step)
        combo_logfname = os.path.join(
            self.tmppath, '{}.log'.format(combo_logbasename))
        combo_logger = get_logger(combo_logbasename, combo_logfname)
        self.logger = combo_logger
        
        merger = BisonMerger(combo_logger)
        # Pull metadata from ticket first, 
        # if missing, fill with old provider table data second
        if os.path.exists(bisonprov_lut_fname):
            self.bisonprov_dataload = Lookup.init_from_file(
                bisonprov_lut_fname, ['legacy_id'], BISON_DELIMITER, 
                VAL_TYPE.DICT, ENCODING)
        else:
            old_resources = merger.read_resources(self.merged_resource_lut_fname)
            bisonprov_dataload_metadata = merger.assemble_files(
                self.workpath, old_resources)
            self.bisonprov_dataload = Lookup.init_from_dict(
                bisonprov_dataload_metadata)
            self.bisonprov_dataload.write_lookup(
                bisonprov_lut_fname, None, BISON_DELIMITER)

    # ...............................................
    def set_bisonprov(self, resource_pvals):
        fname = resource_pvals['filename']
        self.action = resource_pvals['action']
         
        if self.action not in PROVIDER_ACTIONS:
            self.logger.info('Wait to process {}'.format(resource_key))
        else:
            if not fname:
                # filename w/o path or extension
                self.basefname = 'bison_{}'.format(resource_key)
            else:
                # New input files have .txt extension
                self.basefname = os.path.basename(fname)
            self.logger.info('{}: {} {}'.format(
                resource_key, self.action, fname))
            infile = os.path.join(self.basepath, fname)
            if not os.path.exists(infile):
                raise Exception('File {} does not exist for {}'
                                .format(fname, resource_key))
            self.pass1_fname = os.path.join(self.s1dir, '{}.csv'.format(self.basefname))
            self.pass3_fname = os.path.join(self.s3dir, '{}.csv'.format(self.basefname))            
            self.pass4_fname = os.path.join(self.s4dir, '{}.csv'.format(self.basefname))
            
#             # ..........................................................
#             # Step 1: rewrite, handle quotes, fill ticket/constant vals for 
#             # resource/provider
#             # Same process as GBIF Step 1, different resource/provider inputs
#             if step == 1 and not ignore_me:
#                 if os.path.exists(outfile1):
#                     combo_logger.info('  Existing step 1 output in {}'.format(
#                         outfile1))
#                 else:
#                     combo_logger.info('  Process step 1 output to {}'.format(
#                         outfile1))
#                     merger.reset_logger(s1dir, logbasename)
#                     in_delimiter = PROVIDER_DELIMITER
#                     if action in ('rename', 'rewrite'):
#                         in_delimiter = BISON_DELIMITER
#                     merger.rewrite_bison_data(
#                         infile, outfile1, resource_key, resource_pvals, 
#                         in_delimiter)
#             # ..........................................................
#             # Step 2: Nope
#             elif step == 2:
#                 combo_logger.info('Step 2 is only valid for GBIF data')
#             # ..........................................................
#             # Step 3: fill lookup vals from ITIS, establishment_means and county centroid files
#             # Identical to GBIF Step 3
#             elif step == 3 and not ignore_me:
#                 if os.path.exists(outfile3):
#                     combo_logger.info('  Existing step 3 output in {}'.format(
#                         outfile3))
#                 else:
#                     combo_logger.info('  Process step 3 output to {}'.format(
#                         outfile3))
#                     logfname = os.path.join(s3dir, '{}.log'.format(logbasename))
#                     logger = get_logger(logbasename, logfname)
#                     bfiller = BisonFiller(logger)
#                     bfiller.update_itis_estmeans_centroid(
#                         itis2_lut_fname, estmeans_fname, terrestrial_shpname, 
#                         outfile1, outfile3, from_gbif=False)
#             # ..........................................................
#             # Step 4: split and process large files in parallel
#             # Identical to GBIF Step 4
#             elif step == 4 and not ignore_me:
#                 if os.path.exists(outfile4):
#                     combo_logger.info('  Existing step 4 output in {}'.format(
#                         outfile4))
#                 else:
#                     lcount = get_line_count(outfile3)
#                     if lcount < LOGINTERVAL:
#                         combo_logger.info('  Process step 4 output to {}'.format(
#                             outfile4))
#                         logfname = os.path.join(s3dir, '{}.log'.format(logbasename))
#                         logger = get_logger(logbasename, logfname)
#                         bfiller = BisonFiller(logger)
#                         bfiller.update_point_in_polygons(
#                             terr_data, marine_data, ancillary_path, outfile3, outfile4)
#                     else:
#                         combo_logger.info('  Parallel Process step 4 ({} lines) output to {}'
#                                     .format(lcount, outfile4))
#                         # Create chunk input files for parallel processing
#                         csv_filename_pairs, header = get_chunk_files(
#                              outfile3, out_csv_filename=outfile4)
#                         step_parallel(outfile3, terr_data, marine_data, ancillary_path,
#                                       outfile4, from_gbif=False)
#             # ..........................................................
#             # Step 99: fix something
#             elif step == 99 and not ignore_me:
#                 outfile = os.path.join(outpath, basefname)
#                 if not os.path.exists(outfile):
#                     combo_logger.info('  Re-write step 4 output with fix to {}'.format(
#                         outfile))
#                     merger.reset_logger(fixdir, logbasename)
#                     merger.fix_bison_data(
#                         outfile4, outfile, resource_key, resource_pvals)
#             # ..........................................................
#             # Step 1000: test something
#             elif step == 1000 and not ignore_me:
#                 outfile = os.path.join(outpath, basefname)
#                 if os.path.exists(outfile):
#                     combo_logger.info('  Test output {}'.format(outfile))
#                     merger.test_bison_data(outfile, resource_key, resource_pvals)
# 
#             else:
#                 print('Ignore {} {}'.format(resource_key, fname))

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
def do_track_providers(resource_count_fname, provider_count_fname):
    # If both exist, do nothing
    if (os.path.exists(resource_count_fname) and 
        os.path.exists(provider_count_fname)):
        print('Files {} and {} exist'.format(
            resource_count_fname, provider_count_fname))
        track_providers = False
    else:
        track_providers = True
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
       1: Initial transform, checks for both GBIF and BISON-provider datafiles
          * for GBIF data files, Create lookup tables:
              * Resource/Provider lookup
                * Query GBIF dataset API + datasetKey for dataset info for Bison 
                  'resource' fields and (publishing)OrganizationKey.
                * Query GBIF organization API + organizationKey for organization  
                  info for BISON 'provider' fields'
              * Name info (provided_scientific_name, taxonKey) and UUIDs are saved 
                in records for GBIF API resolution in Step 2.
          * for both GBIF and BISON-provider
              * Ecape internal quotes and removing enclosing quotes
              * Check longitude and negate if needed for US and Canada records
              * Fill resource/provider fields from 
                * current GBIF metadata merged with BISON tables (for GBIF data)
                * new or modified metadata from Jira tickets (for BISON provider data)
       2: for GBIF data files only, 
          * Create name lookup table
              * Query GBIF parser + scientificName if available, 
                or GBIF species API + taxonKey --> name lookup
          * Fill clean_provided_scientific_name field with values saved in 
            name lookup table.
       3: Fill ITIS, Establishment_means, centroid coordinates s for records 
          missing coords; for both GBIF and BISON-provider datafiles
          * ITIS fields - resolve with ITIS lookup and
            clean_provided_scientific_name filled in step2
          * establishment_means - resolve with establishment
            means lookup and ITIS TSN or 
            clean_provided_scientific_name
          * longitude / latitude and centroid for records without lon/lat and 
            with state+county or fips from terrestrial centroid coordinates
       4: Fill fields:
          * geo fields: calculate point-in-polygon for records with new or 
            existing lon/lat 
            *  resolve 1st with terrestrial shapefile US_CA_Counties_Centroids, 
               filling calculated_state, calculated_county, calculated_fips
            * or resolve 2nd with marine shapefile 
              World_EEZ_v8_20140228_splitpolygons filling calculated_waterbody 
              and mrgid iff terrestrial georef returns 0 or > 1 result
       99: Fixdata
       1000: Test data
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
                        For steps 1-4: Full path and filename of original 
                          GBIF occurrence file or directory containing files for 
                          BISON provider merge.  
                        For step 99 (fix) or 1000 (test): the file to be fixed 
                          or tested.
                        """)
    parser.add_argument(
        '--step', type=int, default=1, choices=[1,2,3,4,99,1000], help=stephelp)
    parser.add_argument(
        '--resource', type=str, default=None, 
        help='resource_id for testing or processing of a single bison-provider dataset')
    parser.add_argument(
        '--teststep', type=str, default=None, 
        help='step for which to test/examine output')
    args = parser.parse_args()
    data_source = args.data_source
    input_data = args.input_data
    step = args.step
    teststep = args.teststep
    resource_id = args.resource
    dl = DataLoader(data_source, input_data, step, resource_id=resource_id)

    if data_source == 'gbif':
        start_time = time.time()
        if step == 1:
            dl.process_gbif_step1()
        elif step == 2:
            dl.process_gbif_step2()
        elif step == 3:
            dl.process_gbif_step3()
        elif step == 4:
            dl.process_gbif_step4()
        else:
            print('No step {} for data_source {}'.format(step, data_source))
            
        # Log time elapsed for steps 1-4 
        minutes = (time.time() - start_time) / 60
        try:
            dl.logger.info('Elapsed minutes {} for step {}, file {}'.format(
                minutes, step, input_data))
        except:
            print('Elapsed minutes {} for step {}, file {}'.format(
                minutes, step, input_data))
            
            
    elif data_source == 'provider':
        dl.prep_bisonprov()
        # 1066: USDA_PLANTS, 100045: rewrite, 100061: replace, 
        # 100032: replace_rename, emammal: add, nplichens: add
#         FIXME = ['440,1066', '440,100045', '440,100061', 'emammal', 'nplichens']
        FIXME = []
        for resource_key, resource_pvals in dl.bisonprov_dataload.lut.items():
            start_time = time.time()
            ignore_me = False
            # resource_id from command option
            if resource_id is not None and resource_id != resource_key:
                ignore_me = True
            elif resource_key not in FIXME: 
                ignore_me = True
            dl.set_bisonprov(resource_pvals)
"""


import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import time

from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, PROVIDER_ACTIONS, LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, 
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


# ..........................................................


"""
