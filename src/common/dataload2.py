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
import time

from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, BISON_PROVIDER_VALUES, BISON_IPT_PREFIX, IPT_QUERY, 
    LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, MERGED_RESOURCE_LUT_FIELDS,
    OUTPUT_DIR, PROVIDER_DELIMITER, PROVIDER_ACTIONS, ENCODING)
from common.inputdata import ANCILLARY_FILES
from common.lookup import Lookup, VAL_TYPE
from common.tools import (get_logger, get_line_count)
from common.geo_intersect import step_parallel
from gbif.gbifmod import GBIFReader
from provider.providermod import BisonMerger

# .............................................................................
class DataLoader(object):
    """
    Object for processing a Bison Provider or GBIF CSV files filling ITIS fields, 
    coordinates, terrestrial or marine boundaries, and establishment_means.
    """
    # ...............................................
    def __init__(self, data_source, input_data, resource_id=None):
        """
        @summary: Constructor
        """    
        if not os.path.exists(input_data):
            raise Exception('Input file {} does not exist'.format(input_data))
        elif os.path.isdir(input_data):
            self.workpath = input_data.rstrip(os.path.sep)
            self.rawdata_fname = None
        elif os.path.isfile(input_data):
            self.rawdata_fname = input_data
            self.workpath, basefname_wext = os.path.split(self.rawdata_fname)
                
        self._test_resource_id = resource_id
        # basepath contains bison provider and GBIF data
        # workpath is specific to dataset and contains raw data files
        # <some_path>/basepath/workpath
        self.basepath, _ = os.path.split(self.workpath)
        self.ancillary_path = os.path.join(self.basepath, ANCILLARY_DIR)
        self.tmppath = os.path.join(self.workpath, TEMP_DIR)
        self.outpath = os.path.join(self.workpath, OUTPUT_DIR)
        os.makedirs(self.tmppath, mode=0o775, exist_ok=True)
        os.makedirs(self.outpath, mode=0o775, exist_ok=True)

        self.logger = self._get_logger_for_processing(self.tmppath, basefname='dataloader')
    
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
        
        self.merged_resource_lut_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['resource']['latest_file'])
        self.merged_provider_lut_fname = os.path.join(
            self.ancillary_path, ANCILLARY_FILES['provider']['latest_file'])

        self.resource_count_fname = os.path.join(
            self.outpath,  '{}.count.resource.csv'.format(data_source))
        self.provider_count_fname = os.path.join(
            self.outpath,  '{}.count.provider.csv'.format(data_source))
        self.bisonprov_dataload_fname = os.path.join(
            self.ancillary_path, 'bisonprovider_meta_lut.csv')

        self.s1dir = os.path.join(self.tmppath, 's1-rewrite')
        self.s2dir = os.path.join(self.tmppath, 's2-scinames')
        self.s3dir = os.path.join(self.tmppath, 's3-itisescent')
        self.s4dir = os.path.join(self.tmppath, 's4-geo')
        # No step 2 for BISON provider data
        for sdir in (self.s1dir, self.s3dir, self.s4dir):
            os.makedirs(sdir, mode=0o775, exist_ok=True)
        if data_source == 'gbif':
            os.makedirs(self.s2dir, mode=0o775, exist_ok=True)
        
        # All filled in prep_gbif or prep_bisonprov/set_bisonprov function
        self.basefname = None
        self.nametaxa_fname = None
        self.canonical_lut_fname = None        
        self.pass1_fname = None
        self.pass2_fname = None
        self.pass3_fname = None
        self.pass4_fname = None
        self.action = None
        self.bisonprov_dataload = None
        self.track_providers = None 
        
        if data_source == 'gbif':
            self.prep_gbif(resource_lut_fname, provider_lut_fname, basefname_wext)
        elif data_source == 'provider':
            self.prep_bisonprov()
        elif data_source == 'all':
            self.create_providers_resources(resource_lut_fname, provider_lut_fname)
            self.prep_bisonprov()
        
    # ...............................................
    def _get_logger_for_processing(self, absfilename_or_path, basefname=None):
        if basefname is None:
            basefname = self.basefname
        if os.path.isdir(absfilename_or_path):
            pth = absfilename_or_path
        else:
            pth, _ = os.path.split(absfilename_or_path)
        logfname = os.path.join(pth, '{}.log'.format(basefname))
        logger = get_logger(basefname, logfname)
        return logger
            
    # ...............................................
    def prep_gbif(self, resource_lut_fname, provider_lut_fname, basefname_wext):
        # filename w/o path or extension
        self.basefname, _ = os.path.splitext(basefname_wext)
        self.nametaxa_fname = os.path.join(self.tmppath, 'step1_{}_sciname_taxkey_list.csv'
                                      .format(self.basefname))
        self.canonical_lut_fname = os.path.join(self.tmppath, 'step2_{}_canonical_lut.csv'
                                           .format(self.basefname))        
        self.track_providers = self._do_track_providers()
        # Output CSV files of all records after initial creation or field replacements
        self.pass1_fname = os.path.join(self.s1dir, '{}.csv'.format(self.basefname))
        self.pass2_fname = os.path.join(self.s2dir, '{}.csv'.format(self.basefname))
        self.pass3_fname = os.path.join(self.s3dir, '{}.csv'.format(self.basefname))
        self.pass4_fname = os.path.join(self.s4dir, '{}.csv'.format(self.basefname))

        if (not os.path.exists(self.merged_provider_lut_fname) or
            not os.path.exists(self.merged_resource_lut_fname)):
            gr = GBIFReader(self.workpath, self.logger)
            # Merge existing provider and resource metadata with GBIF dataset 
            # metadata files and GBIF API-returned metadata
            gr.resolve_provider_resource_for_lookup(
                self.merged_resource_lut_fname, resource_lut_fname, 
                self.merged_provider_lut_fname, provider_lut_fname, 
                outdelimiter=BISON_DELIMITER)

    # ...............................................
    def prep_bisonprov(self):
        task_logger = self._get_logger_for_processing('assemble_bprov_dataload')
        merger = BisonMerger(task_logger)
        # if missing, fill with old provider table data second
        if os.path.exists(self.bisonprov_dataload_fname):
            self.bisonprov_dataload = Lookup.init_from_file(
                self.bisonprov_dataload_fname, ['legacy_id'], BISON_DELIMITER, 
                VAL_TYPE.DICT, ENCODING)
        else:
            old_resources = merger.read_resources(self.merged_resource_lut_fname)
            bisonprov_dataload_metadata = merger.assemble_files(
                self.workpath, old_resources)
            self.bisonprov_dataload = Lookup.init_from_dict(
                bisonprov_dataload_metadata)
            self.bisonprov_dataload.write_lookup(
                self.bisonprov_dataload_fname, None, BISON_DELIMITER)

    # ...............................................
    def reset_bisonprov(self, resource_key, resource_pvals):
        self.action = resource_pvals['action']
        
        fname = resource_pvals['filename']                 
        if not fname:
            # filename w/o path or extension
            self.basefname = 'bison_{}'.format(resource_key)
            self.rawdata_fname = os.path.join(
                self.workpath, '{}.csv'.format(self.basefname))
        else:
            # New input files have .txt extension
            self.basefname, _ = os.path.splitext(fname)
            self.rawdata_fname = os.path.join(self.workpath, fname)
            
        self.logger.info('{}: {} {}'.format(
            resource_key, self.action, fname))
        if not os.path.exists(self.rawdata_fname):
            raise Exception('File {} does not exist for {}'
                            .format(fname, resource_key))
        self.pass1_fname = os.path.join(self.s1dir, '{}.csv'.format(self.basefname))
        self.pass3_fname = os.path.join(self.s3dir, '{}.csv'.format(self.basefname))            
        self.pass4_fname = os.path.join(self.s4dir, '{}.csv'.format(self.basefname))
        
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
    def create_providers_resources(self, resource_lut_fname, provider_lut_fname):
        # Step 1: assemble metadata, initial rewrite to BISON format, (GBIF-only)
        # fill resource/prov0ider, check coords (like provider step 1)
        logger = self._get_logger_for_processing(self.ancillary_path, 'create_prov_res')
        gr = GBIFReader(self.workpath, self.logger)

        # Update resource, provider filenames to force new creation
        today = time.localtime()
        dtstr = '{}.{}.{}'.format(today.tm_year, today.tm_mon, today.tm_mday)
        self.merged_resource_lut_fname = os.path.join(
            self.ancillary_path, 'provider_table.{}.csv'.format(dtstr))
        self.merged_provider_lut_fname = os.path.join(
            self.ancillary_path, 'provider_table.{}.csv'.format(dtstr))
        logger.info("Update 'latest_file' in ANCILLARY_FILES 'provider' and 'resource'")
        
        gr.resolve_provider_resource_for_lookup(
            self.merged_resource_lut_fname, resource_lut_fname, 
            self.merged_provider_lut_fname, provider_lut_fname, 
            outdelimiter=BISON_DELIMITER)
        
        gr = GBIFReader(self.workpath, logger)
        gr.write_resource_provider_stats(
            self.resource_count_fname, self.provider_count_fname, overwrite=True)

        
    # ...............................................
    def process_gbif_step1(self):
        # Step 1: assemble metadata, initial rewrite to BISON format, (GBIF-only)
        # fill resource/prov0ider, check coords (like provider step 1)
        if not os.path.exists(self.pass1_fname):
            task_logger = self._get_logger_for_processing(self.pass1_fname)
            gr = GBIFReader(self.workpath, task_logger)
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
        else:
            self.logger.info('Output file for step 1 exists: {}'.format(
                self.pass1_fname))

    # ...............................................
    def process_gbif_step2(self):
        # Step 2: assemble and parse names, rewrite records with clean names
        if not os.path.exists(self.pass2_fname):
            task_logger = self._get_logger_for_processing(self.pass2_fname)
            gr = GBIFReader(self.workpath, task_logger)
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(self.nametaxa_fname):
                gr.gather_name_input(self.pass1_fname, self.nametaxa_fname)
                
            canonical_lut = gr.resolve_canonical_taxonkeys_for_lookup(
                self.nametaxa_fname, self.canonical_lut_fname)
            # Discard records with no clean name
            gr.update_bison_names(
                self.pass1_fname, self.pass2_fname, canonical_lut, 
                track_providers=self.track_providers)
        else:
            self.logger.info('Output file for step 2 exists: {}'.format(
                self.pass2_fname))

    # ...............................................
    def process_gbif_step3(self):
        # Step 3: rewrite records filling ITIS fields, establishment_means, and 
        # county centroids for records without coordinates 
        if not os.path.exists(self.pass3_fname):
            task_logger = self._get_logger_for_processing(self.pass3_fname)
            bf = BisonFiller(task_logger)
            # No discards
            bf.update_itis_estmeans_centroid(
                self.itis2_lut_fname, self.estmeans_fname, self.terrestrial_shpname, 
                self.pass2_fname, self.pass3_fname, from_gbif=True,
                track_providers=self.track_providers)
        else:
            self.logger.info('Output file for step 3 exists: {}'.format(
                self.pass3_fname))

    # ...............................................
    def process_gbif_step4(self):
        if not os.path.exists(self.pass4_fname):
            # Step 4: split into smaller files, parallel process 
            # Identify enclosing terrestrial or marine polygons, rewrite with 
            # calculated state/county/fips or mrgid/eez 
            terr_data = ANCILLARY_FILES['terrestrial']
            marine_data = ANCILLARY_FILES['marine']
            # No discards
            lcount = get_line_count(self.pass3_fname)
            if lcount < LOGINTERVAL:
                self.logger.info('  Process step 4 output to {}'.format(
                    self.pass4_fname))
                task_logger = self._get_logger_for_processing(self.pass4_fname)
                bf = BisonFiller(task_logger)
                bf.update_point_in_polygons(
                    terr_data, marine_data, self.ancillary_path, 
                    self.pass3_fname, self.pass4_fname)
            else:
                step_parallel(
                    self.pass3_fname, terr_data, marine_data, self.ancillary_path, 
                    self.pass4_fname, from_gbif=True)
        else:
            self.logger.info('Output file for step 4 exists: {}'.format(
                self.pass4_fname))


#     # ...............................................
#     def process_gbif_fix(self):
#         # Step 99: fix something
#         infile = os.path.join(self.pass4_fname, 'occurrence_lines_1-2000.csv')
#         task_logger = self._get_logger_for_processing(self.pass4_fname)
#         fixfile = os.path.join(fixdir, 'occurrence_lines_1-2000.csv')
#         logfname = os.path.join(fixdir, '{}.log'.format(basefname))
#         logger = get_logger(basefname, logfname)
#         bf = BisonFiller(logger)
#         bf.rewrite_recs(infile, fixfile, BISON_DELIMITER)
# 
#     # ...............................................
#     def process_gbif_test(self):
#         # Step 1000: test something
#         outfile = os.path.join(outpath, '{}.csv'.format(basefname))
#         if os.path.exists(outfile):
#             logfname = os.path.join(outpath, '{}.log'.format(basefname))
#             logger = get_logger(basefname, logfname)
#             bfiller = BisonFiller(logger)
#             bfiller.walk_data(
#                 in_fname, terr_data, marine_data, ancillary_path, 
#                 merged_resource_lut_fname, merged_provider_lut_fname)

    # ...............................................
    def fill_bisonprov_columns(self, base_outfname):
        resource_url_prefix = '{}/{}'.format(BISON_IPT_PREFIX, IPT_QUERY)
        provider_id = BISON_PROVIDER_VALUES['provider_id']
        provider_legacy_id = BISON_PROVIDER_VALUES['provider_legacy_id']
        provider_url = BISON_PROVIDER_VALUES['provider_url']
        
        task_logger = self._get_logger_for_processing('fill_bisonprov_columns')
        merger = BisonMerger(task_logger)
        resources = merger.read_resources(self.merged_resource_lut_fname)

        for resource_key, resource_pvals in self.bisonprov_dataload.lut.items():
            action = resource_pvals['action']
            if action in PROVIDER_ACTIONS:
                try:
                    vals = resources.lut[resource_key]
                except:
                    vals = {}
                res_legacy_id = ''    
                if resource_key.startswith('440,'):
                    res_legacy_id = resource_key.split(',')[1]
                # Construct from inputdata.py
                vals['bison_resource_uuid'] = resource_pvals['resource_id']
                vals['bison_resource_legacy_id'] = res_legacy_id
                vals['bison_resource_name'] = resource_pvals['resource_name']
                vals['bison_resource_url'] = '{}{}'.format(
                    resource_url_prefix, resource_pvals['resource_id'])
                # Constants
                vals['bison_provider_uuid'] = provider_id
                vals['bison_provider_legacy_id'] = provider_legacy_id
                vals['bison_provider_name'] = provider_id
                vals['bison_provider_url'] = provider_url
                resources.lut[resource_key] = vals
            else:
                dl.logger.info('Ignore dataset {} with action {}'.format(
                    resource_key, action))

        # Save dataset information
        header = [fld for (fld, _) in MERGED_RESOURCE_LUT_FIELDS]
        new_resource_fname = os.path.join(dl.ancillary_path, base_outfname)
        resources.write_lookup(new_resource_fname, header, BISON_DELIMITER)
        task_logger.info('Wrote dataset metadata to {}'.format(
            new_resource_fname))

    # ...............................................
    def process_bisonprov_step1(self):
        if not os.path.exists(self.pass1_fname):
            # Step 1: rewrite, handle quotes, fill ticket/constant vals for 
            # resource/provider    
            # Same process as GBIF Step 1, different resource/provider inputs
            self.logger.info('  Process step 1 output to {}'.format(
                self.pass1_fname))
            task_logger = self._get_logger_for_processing(self.pass1_fname)
            merger = BisonMerger(task_logger)

#             # if missing, fill with old provider table data second
#             bisonprov_lut_fname = os.path.join(
#                 self.ancillary_path, 'bisonprovider_meta_lut.csv')
#             if os.path.exists(bisonprov_lut_fname):
#                 self.bisonprov_dataload = Lookup.init_from_file(
#                     bisonprov_lut_fname, ['legacy_id'], BISON_DELIMITER, 
#                     VAL_TYPE.DICT, ENCODING)
#             else:
#                 old_resources = merger.read_resources(self.merged_resource_lut_fname)
#                 bisonprov_dataload_metadata = merger.assemble_files(
#                     self.workpath, old_resources)
#                 self.bisonprov_dataload = Lookup.init_from_dict(
#                     bisonprov_dataload_metadata)
#                 self.bisonprov_dataload.write_lookup(
#                     bisonprov_lut_fname, None, BISON_DELIMITER)

            # Standardize provider/resource and with first pass checks
            in_delimiter = PROVIDER_DELIMITER
            if self.action in ('rename', 'rewrite'):
                in_delimiter = BISON_DELIMITER
            merger.rewrite_bison_data(
                self.rawdata_fname, self.pass1_fname, resource_key, 
                resource_pvals, in_delimiter)
        else:
            self.logger.info('Output file for step 1 exists: {}'.format(
                self.pass1_fname))
            
            
#             # ..........................................................
#             # Step 2: Nope
#             elif step == 2:
#                 combo_logger.info('Step 2 is only valid for GBIF data')
    # ...............................................
    def process_bisonprov_step3(self):
        if not os.path.exists(self.pass3_fname):
            # Step 3: fill lookup vals from ITIS, establishment_means and county centroid files
            # Identical to GBIF Step 3
            self.logger.info('  Process step 3 output to {}'.format(
                self.pass3_fname))
            task_logger = self._get_logger_for_processing(self.pass3_fname)
            bfiller = BisonFiller(task_logger)
            bfiller.update_itis_estmeans_centroid(
                self.itis2_lut_fname, self.estmeans_fname, 
                self.terrestrial_shpname, self.pass1_fname, self.pass3_fname, 
                from_gbif=False)
        else:
            self.logger.info('Output file for step 3 exists: {}'.format(
                self.pass3_fname))

    # ...............................................
    def process_bisonprov_step4(self):
        if not os.path.exists(self.pass4_fname):
            # Step 4: split and process large files in parallel
            # Identical to GBIF Step 4
            terr_data = ANCILLARY_FILES['terrestrial']
            marine_data = ANCILLARY_FILES['marine']

            lcount = get_line_count(self.pass3_fname)
            if lcount < LOGINTERVAL:
                self.logger.info('  Process step 4 output to {}'.format(
                    self.pass4_fname))
                task_logger = self._get_logger_for_processing(self.pass4_fname)
                bfiller = BisonFiller(task_logger)
                bfiller.update_point_in_polygons(
                    terr_data, marine_data, self.ancillary_path, 
                    self.pass3_fname, self.pass4_fname)
            else:
                self.logger.info('  Parallel Process step 4 ({} lines) output to {}'
                            .format(lcount, self.pass4_fname))
                # Create chunk input files for parallel processing
#                 csv_filename_pairs, header = get_chunk_files(
#                      self.pass3_fname, out_csv_filename=self.pass4_fname)
                step_parallel(
                    self.pass3_fname, terr_data, marine_data, 
                    self.ancillary_path, self.pass4_fname, from_gbif=False)
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
        '--step', type=int, default=1, choices=[0, 1,2,3,4,99,1000], help=stephelp)
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
    test_resource_id = args.resource
    dl = DataLoader(data_source, input_data, resource_id=test_resource_id)
    
    start_time = time.time()
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
        elif step == 0:
            dl.process_gbif_step1()
            dl.process_gbif_step2()
            dl.process_gbif_step3()
            dl.process_gbif_step4()
        else:
            print('No step {} for data_source {}'.format(step, data_source))
            
    elif data_source == 'provider':
        if step == 1000:
            dl.create_providers_resources(resource_lut_fname, provider_lut_fname)
        else:
            SEE_ONLY_ME = None
            for resource_key, resource_pvals in dl.bisonprov_dataload.lut.items():
                this_start_time = time.time()
                # 2 ways to limit processing 
                # resource_id from command option
                ignore_me = False
                if test_resource_id is not None and test_resource_id != resource_key:
                    ignore_me = True
                elif SEE_ONLY_ME and resource_key not in SEE_ONLY_ME: 
                    ignore_me = True
                    
                action = resource_pvals['action']
                if action in PROVIDER_ACTIONS and not ignore_me:
                    dl.reset_bisonprov(resource_key, resource_pvals)
                    if step == 1:
                        dl.process_bisonprov_step1()
                    elif step == 3:
                        dl.process_bisonprov_step3()
                    elif step == 4:
                        dl.process_bisonprov_step4()
                    elif step == 0:
                        dl.process_bisonprov_step1()
                        dl.process_bisonprov_step3()
                        dl.process_bisonprov_step4()
                    else:
                        print('No step {} for data_source {}'.format(step, data_source))
                else:
                    dl.logger.info('Ignore dataset {} with action {}'.format(
                        resource_key, action))
    
                these_minutes = (time.time() - this_start_time) / 60
                dl.logger.info('Elapsed minutes {} for step {}, resource {}'.format(
                    these_minutes, step, resource_key))
    
        all_minutes = (time.time() - start_time) / 60
        try:
            dl.logger.info('Elapsed minutes {} for step {}, input {}'.format(
                all_minutes, step, input_data))
        except:
            print('Elapsed minutes {} for step {}, input {}'.format(
                all_minutes, step, input_data))

"""
import os
import time
from common.bisonfill import BisonFiller
from common.constants import (
    BISON_DELIMITER, BISON_PROVIDER_VALUES, BISON_IPT_PREFIX, IPT_QUERY, 
    LOGINTERVAL, ANCILLARY_DIR, TEMP_DIR, MERGED_RESOURCE_LUT_FIELDS,
    OUTPUT_DIR, PROVIDER_DELIMITER, PROVIDER_ACTIONS, ENCODING)
from common.inputdata import ANCILLARY_FILES
from common.lookup import Lookup, VAL_TYPE
from common.tools import (get_logger, get_line_count)
from common.geo_intersect import step_parallel
from gbif.gbifmod import GBIFReader
from provider.providermod import BisonMerger
from common.dataload2 import *

data_source = 'gbif'
input_data = '/tank/data/bison/2019/CA_USTerr_gbif/occurrence_lines_20000001-30000001.csv'


data_source = 'provider'
input_data = '/tank/data/bison/2019/provider'
step = 1000

dl = DataLoader(data_source, input_data)



# ..........................................................


"""
