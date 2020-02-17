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

from common.bisonfill import BisonFiller
from common.constants import BISON_DELIMITER, BISON_PROVIDER, ProviderActions
from common.tools import getLogger

from gbif.gbifmod import GBIFReader

from provider.providermod import BisonMerger
            
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
    parser.add_argument('--step', type=int, default=1, choices=[1,2,3,4,5,10],
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
    if step in [1,2,3,4,10]:
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
    ancillary_path = os.path.join(datapth, 'ancillary')
    terrestrial_shpname = os.path.join(ancillary_path, 'US_CA_Counties_Centroids.shp')
    estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
    marine_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
    itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')
    resource_lut_fname = os.path.join(ancillary_path, 'resourcestable.csv')
    provider_lut_fname = os.path.join(ancillary_path, 'providerstable.csv')
        
    if step in [1,2,3,4]:
        # Files of name lookup and list for creation 
        nametaxa_fname = os.path.join(tmppath, 'step1_{}_sciname_taxkey_list.csv'.format(basefname))
        canonical_lut_fname = os.path.join(tmppath, 'step2_{}_canonical_lut.csv'.format(basefname))        
        # Output CSV files of all records after initial creation or field replacements
        pass1_fname = os.path.join(tmppath, 'step1_{}.csv'.format(basefname))
        pass2_fname = os.path.join(tmppath, 'step2_{}.csv'.format(basefname))
        pass3_fname = os.path.join(tmppath, 'step3_{}.csv'.format(basefname))
        pass4_fname = os.path.join(tmppath, 'step4_{}.csv'.format(basefname))    
        # filenames for dataset/resource and organization/provider lookups
        dataset_lut_fname = os.path.join(tmppath, 'dataset_lut.csv')
        org_lut_fname = os.path.join(tmppath, 'organization_lut.csv')
    
    if not os.path.exists(occ_file_or_path):
        raise Exception('File or path {} does not exist'.format(occ_file_or_path))
    else:
        logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
        logger = getLogger(logbasename, logfname)
        if step == 1:
            gr = GBIFReader(inpath, tmpdir, outdir, logger)
            gr.write_dataset_org_lookup(dataset_lut_fname, resource_lut_fname, 
                                        org_lut_fname, provider_lut_fname, 
                                        outdelimiter=BISON_DELIMITER)
            # Pass 1 of CSV transform, initial pull, standardize, 
            gr.transform_gbif_to_bison(occ_file_or_path, dataset_lut_fname, 
                                       org_lut_fname, nametaxa_fname, 
                                       pass1_fname)
            
        elif step == 2:
            gr = GBIFReader(inpath, tmpdir, outdir, logger)
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(nametaxa_fname):
                gr.gather_name_input(pass1_fname, nametaxa_fname)
                
            canonical_lut = gr.get_canonical_lookup(nametaxa_fname, 
                                                    canonical_lut_fname)
            # Pass 2 of CSV transform
            gr.update_bison_names(pass1_fname, pass2_fname, canonical_lut)
            
        elif step == 3:
            bf = BisonFiller(pass2_fname, log=logger)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.update_itis_estmeans_centroid(itis2_lut_fname, estmeans_fname, 
                                             terrestrial_shpname, pass3_fname, 
                                             fromGbif=True)
        elif step == 4:
            bf = BisonFiller(pass3_fname, log=logger)
            # Pass 4 of CSV transform, final step, point-in-polygon intersection
            bf.update_point_in_polygons(ancillary_path, pass4_fname, fromGbif=True)
            
        elif step == 5:
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
                    outfile2 = os.path.join(tmppath, basename + '_itis_em_geo.csv')            
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
                        bf.update_point_in_polygons(ancillary_path, outfile3, fromGbif=False)

        elif step == 10:
            bf = BisonFiller(pass3_fname, log=logger)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.test_bison_outfile(fromGbif=True)
"""

# /tank/data/bison/2019/Terr/occurrence_lines_5000-10000.csv --step=4

inpath = '/tank/data/bison/2019/provider'

for resource_id, pvals in BISON_PROVIDER.items():
    fname = pvals['filename']
    if fname is None:
        fname = 'bison_{}.csv'.format(resource_id)
    infile = os.path.join(inpath, fname)
    if not os.path.exists(infile):
        print('File {} does not exist for {}'
              .format(fname, resource_id))

wc -l occurrence.txt 
71057978 occurrence.txt
wc -l tmp/step1.csv 
1577732 tmp/step1.csv

python3.6 /state/partition1/git/bison/src/gbif/convert2bison.py /tank/data/bison/2019/Terr/occurrence_lines_1-10000001.csv --step=1

import os
from osgeo import ogr 
import time

from gbif.constants import (GBIF_DELIMITER, BISON_DELIMITER, PROHIBITED_VALS, 
                            TERM_CONVERT, ENCODING, META_FNAME,
                            BISON_GBIF_MAP, OCC_ID_FLD, DISCARD_FIELDS,
                            CLIP_CHAR, GBIF_UUID_KEY)
from gbif.gbifmeta import GBIFMetaReader
from common.tools import (getCSVReader, getCSVDictReader, 
                        getCSVWriter, getCSVDictWriter, getLine, getLogger)
from gbif.gbifapi import GbifAPI
from pympler import asizeof

ENCODING = 'utf-8'
BISON_DELIMITER = '$'


"""
