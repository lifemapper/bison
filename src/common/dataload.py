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
from common.constants import BISON_DELIMITER
from common.tools import getLogger

from gbif.gbifmodify import GBIFReader
            
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Parse a GBIF occurrence dataset downloaded
                                     from the GBIF occurrence web service in
                                     Darwin Core format into BISON format.  
                                 """))
    parser.add_argument('occ_file', type=str, 
                        help="""
                        Absolute pathname of the input occurrence file 
                        for data transform.  Path contains downloaded GBIF data 
                        and metadata or BISON provider data.  
                        If the subdirectories 'tmp' and 'out' 
                        are not present in the same directory as the raw data, 
                        they will be  created for temp and final output files.
                        """)
    parser.add_argument('--step', type=int, default=1, choices=[1,2,3,4,10],
                        help="""
                        Step number for data processing:
                           1: Only for GBIF data files. 
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
                           10: Test data:
                              - ITIS fields - resolve with ITIS lookup and
                        """)
    args = parser.parse_args()
    occ_fname = args.occ_file
    step = args.step

    overwrite = True
    tmpdir = 'tmp'
    outdir = 'out'
    inpath, basefname_wext = os.path.split(occ_fname)
    # one level up

    datapth, _ = os.path.split(inpath)
    ancillary_path = os.path.join(datapth, 'ancillary')
    basefname, ext = os.path.splitext(basefname_wext)

    tmppath = os.path.join(inpath, tmpdir)
    outpath = os.path.join(inpath, outdir)
    os.makedirs(tmppath, mode=0o775, exist_ok=True)
    os.makedirs(outpath, mode=0o775, exist_ok=True)
    
    # ancillary data for record update
    terrestrial_shpname = os.path.join(ancillary_path, 'US_CA_Counties_Centroids.shp')
    estmeans_fname = os.path.join(ancillary_path, 'NonNativesIndex20190912.txt')
    marine_shpname = os.path.join(ancillary_path, 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp')
    itis2_lut_fname = os.path.join(ancillary_path, 'itis_lookup.csv')
    resource_lut_fname = os.path.join(ancillary_path, 'resourcestable.csv')
    provider_lut_fname = os.path.join(ancillary_path, 'providerstable.csv')
    
    # filename for dataset(gbif) resource(bison) lookup
    dataset_lut_fname = os.path.join(tmppath, 'dataset_lut.csv')
    # filename for organization(gbif) provider(bison) lookup
    org_lut_fname = os.path.join(tmppath, 'organization_lut.csv')
    # filename for scientificname or taxonkey to canonical (clean_provided_scientific_name) lookup
    # for multipart datasets, do a single lut
#     canonical_lut_fname = os.path.join(tmppath, 'canonical_lut.csv')
    canonical_lut_fname = os.path.join(tmppath, 'step2_{}_canonical_lut.csv'.format(basefname))

    itis1_lut_fname = os.path.join(tmppath, 'step3_itis_lut.txt')
    
    logbasename = 'step{}_{}'.format(step, basefname)
    # Files of lookups and lists for their creation 
    nametaxa_fname = os.path.join(tmppath, 'step1_{}_sciname_taxkey_list.csv'.format(basefname))
#     cleanname_fname = os.path.join(tmppath, 'step2_{}_cleanname_list.txt'.format(gbif_basefname))
    # Output CSV files of all records after initial creation or field replacements
    pass1_fname = os.path.join(tmppath, 'step1_{}.csv'.format(basefname))
    pass2_fname = os.path.join(tmppath, 'step2_{}.csv'.format(basefname))
    pass3_fname = os.path.join(tmppath, 'step3_{}.csv'.format(basefname))
    pass4_fname = os.path.join(tmppath, 'step4_{}.csv'.format(basefname))
    
    if not os.path.exists(occ_fname):
        raise Exception('Filename {} does not exist'.format(occ_fname))
    else:
        if step == 1:
            gr = GBIFReader(inpath, tmpdir, outdir, logbasename)
            gr.write_dataset_org_lookup(dataset_lut_fname, resource_lut_fname, 
                                        org_lut_fname, provider_lut_fname, 
                                        outdelimiter=BISON_DELIMITER)
            # Pass 1 of CSV transform, initial pull, standardize, 
            gr.transform_gbif_to_bison(occ_fname, dataset_lut_fname, 
                                       org_lut_fname, nametaxa_fname, 
                                       pass1_fname)
            
        elif step == 2:
            gr = GBIFReader(inpath, tmpdir, outdir, logbasename)
            # Reread output ONLY if missing gbif name/taxkey 
            if not os.path.exists(nametaxa_fname):
                gr.gather_name_input(pass1_fname, nametaxa_fname)
                
            canonical_lut = gr.get_canonical_lookup(nametaxa_fname, 
                                                    canonical_lut_fname)
            # Pass 2 of CSV transform
            gr.update_bison_names(pass1_fname, pass2_fname, canonical_lut)
            
        elif step == 3:
            logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
            log = getLogger(logbasename, logfname)
            bf = BisonFiller(pass2_fname, log=log)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.update_itis_estmeans_centroid(itis2_lut_fname, estmeans_fname, 
                                             terrestrial_shpname, pass3_fname, 
                                             fromGbif=True)
        elif step == 4:
            logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
            log = getLogger(logbasename, logfname)
            bf = BisonFiller(pass3_fname, log=log)
            # Pass 4 of CSV transform, final step, point-in-polygon intersection
            bf.update_point_in_polygons(ancillary_path, pass4_fname, fromGbif=True)
            
        elif step == 10:
            logfname = os.path.join(tmppath, '{}.log'.format(logbasename))
            log = getLogger(logbasename, logfname)
            bf = BisonFiller(pass3_fname, log=log)
            # Pass 3 of CSV transform
            # Use Derek D. generated ITIS lookup itis2_lut_fname
            bf.test_bison_outfile(fromGbif=True)
"""
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
