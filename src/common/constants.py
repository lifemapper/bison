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
from collections import OrderedDict
from enum import Enum, auto

# Test these against lowercase values
PROHIBITED_VALS = ['na', '#na', 'n/a', '']

PROHIBITED_CHARS = ['@', '&', '#', '~', 
                    '*', '\t', '%', '|', '$', '-',]
PROHIBITED_SNAME_CHARS = [',', '\'', '"', '.', '?', 
                          '(', ')', '[', ']', '{', '}']
PROHIBITED_RESOURCE_CHARS = []
BISON_SQUID_FLD = 'occurrence_url'

EXTRA_VALS_KEY = 'rest'


ENCODING = 'utf-8'
ANCILLARY_DELIMITER = ','
BISON_DELIMITER = '$'
PROVIDER_DELIMITER = '\t'

NEWLINE = '\n'
LEGACY_ID_DEFAULT = '-9999'

# Rough log of processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = ' '.join(["%(asctime)s",
                       "%(funcName)s",
                       "line",
                       "%(lineno)d",
                       "%(levelname)-8s",
                       "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000 
LOGFILE_BACKUP_COUNT = 5

ITIS_KINGDOMS = ('animalia', 'plantae', 'bacteria', 'fungi', 'protozoa', 
                 'chromista', 'archaea', 'virus')
ITIS_SOLR_URL = 'https://services.itis.gov/'
ITIS_NAME_KEY = 'nameWOInd'
ITIS_TSN_KEY = 'tsn'
ITIS_URL_ESCAPES = [ [" ", "\%20"] ]
ITIS_VERNACULAR_QUERY = 'https://www.itis.gov/ITISWebService/services/ITISService/getCommonNamesFromTSN?tsn='
ITIS_NAMESPACE = '{http://itis_service.itis.usgs.gov}'
ITIS_DATA_NAMESPACE = '{http://data.itis_service.itis.usgs.gov/xsd}'
W3_NAMESPACE =  '{http://www.w3.org/2001/XMLSchema-instance}'

BISON_IPT_PREFIX = 'https://bison.usgs.gov/ipt'
BISON_IPT_TAIL = 'resource?r='
BISON_VALUES = {'provider': 'BISON',
                'provider_url': 'https://bison.usgs.gov',
                'provider_id': '440',
                'license' : 'http://creativecommons.org/publicdomain/zero/1.0/legalcode',
                }

TEMP_DIR = 'tmp'
OUTPUT_DIR = 'output'
ANCILLARY_DIR = 'ancillary'

ISO_COUNTRY_CODES = ('AS', 'CA', 'FM', 'GU', 'MH', 'MP', 'PR', 'P', 'UM', 'US', 'VI')

class ProviderActions(Enum):
    add = auto()
    rename = auto()
    replace = auto()
    replace_rename = auto()
    rewrite = auto()
    wait = auto()
    unknown = auto()
    
# class BISON_PROVIDER_VALUE(object):
#     legacyid = '440'
#     name = 'BISON'
#     url = 'https://bison.usgs.gov'

class ALLOWED_TYPE(Enum):
    varchar = auto()
    integer = auto()
    double_precision = auto()
    

BISON_ORDERED_DATALOAD_FIELD_TYPE = OrderedDict(
    {'id': {'pgtype': ALLOWED_TYPE.integer},
     'clean_provided_scientific_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'itis_common_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'itis_tsn': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'hierarchy_string':                                    # New field
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 600},  
     'amb': 
     {'pgtype': ALLOWED_TYPE.integer},                      # New field
     'basis_of_record': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 100},
     'occurrence_date': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 100},
     'year': 
     {'pgtype': ALLOWED_TYPE.integer},
     'verbatim_event_date': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'provider': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 255},
     'provider_url': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 255},
     'resource': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 300},
     'resource_url': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 255},
     'occurrence_url': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 255},
     'catalog_number': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 100},
     'collector': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'collector_number': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 500},
     'valid_accepted_scientific_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 300},
     'valid_accepted_tsn': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 300},
     'provided_scientific_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 300},
     'provided_tsn': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 64},
     'latitude': 
     {'pgtype': ALLOWED_TYPE.double_precision},
     'longitude': 
     {'pgtype': ALLOWED_TYPE.double_precision},
     'verbatim_elevation': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'verbatim_depth': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'calculated_county_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 100},
     'calculated_fips': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 5},
     'calculated_state_fips': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 2},
     'calculated_state_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 100},
     'centroid': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 50},
     'provided_county_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 500},
     'provided_fips': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 5},
     'provided_state_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 255},
     'thumb_url': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'associated_media': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'associated_references': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'general_comments': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
    # TODO: ???
     'occurrence_id': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'provider_id': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'resource_id': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 300},
     'provided_common_name': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'kingdom': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 64},
     'geodetic_datum': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 128},
     'coordinate_precision': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 64},
     'coordinate_uncertainty': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 1000},
     'verbatim_locality': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     'mrgid': 
     {'pgtype': ALLOWED_TYPE.integer},
     'calculated_waterbody': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 200},
     'establishment_means': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 40},
     'iso_country_code': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': 2},
     'license':
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None}
    })

class FieldContent(Enum):
    legacy_bison = auto()
    new_bison = auto()
    current_gbif = auto()

MERGED_PROVIDER_LUT_FIELDS = (
    # merged table columns from BISON db table
    ('name', FieldContent.legacy_bison),
    ('provider_url', FieldContent.legacy_bison), 
    ('description', FieldContent.legacy_bison),
    ('website_url', FieldContent.legacy_bison),
    ('created', FieldContent.legacy_bison),
    ('modified', FieldContent.legacy_bison),
    ('deleted', FieldContent.legacy_bison),
    ('display_name', FieldContent.legacy_bison),
    ('BISONProviderID', FieldContent.legacy_bison),
    # Query with legacy identifier second
    ('OriginalProviderID', FieldContent.legacy_bison),          # BISON provider legacy id
    # Query with UUID first
    ('organization_id', FieldContent.legacy_bison),             # GBIF organization UUID
    # merged table columns from GBIF
    ('gbif_organizationKey', FieldContent.current_gbif),
    ('gbif_legacyid', FieldContent.current_gbif),               # --> provider_id field
    ('gbif_title', FieldContent.current_gbif),                  # --> record provider field
    ('gbif_url', FieldContent.current_gbif),                    # --> provider_url field 
    ('gbif_description', FieldContent.current_gbif), 
    ('gbif_citation', FieldContent.current_gbif), 
    ('gbif_created', FieldContent.current_gbif), 
    ('gbif_modified', FieldContent.current_gbif), 
    # NEW merged table columns for BISON db
    ('bison_provider_uuid', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_legacy_id', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_name', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_url', FieldContent.new_bison),                    # --> resource_url field 
)

MERGED_RESOURCE_LUT_FIELDS = (
    # columns from BISON db table
    ('BISONProviderID', FieldContent.legacy_bison), 
    ('name', FieldContent.legacy_bison), 
    ('display_name', FieldContent.legacy_bison), 
    ('description', FieldContent.legacy_bison), 
    ('rights', FieldContent.legacy_bison), 
    ('citation', FieldContent.legacy_bison), 
    ('logo_url', FieldContent.legacy_bison), 
    ('created', FieldContent.legacy_bison), 
    ('modified', FieldContent.legacy_bison), 
    ('deleted', FieldContent.legacy_bison), 
    ('website_url', FieldContent.legacy_bison),
    ('override_citation', FieldContent.legacy_bison), 
    ('provider_id', FieldContent.legacy_bison),                 # BISON provider legacy id
    ('OriginalResourceID', FieldContent.legacy_bison),          # BISON resource legacy id
    ('BISONResourceID', FieldContent.legacy_bison),
    ('dataset_id', FieldContent.legacy_bison),                  # GBIF dataset UUID
    ('owningorganization_id', FieldContent.legacy_bison),       # GBIF organization UUID
    ('provider_url', FieldContent.legacy_bison),
    ('provider_name', FieldContent.legacy_bison),
    # merged table columns from GBIF
    ('gbif_datasetkey', FieldContent.current_gbif),             # GBIF dataset UUID
    ('gbif_legacyid', FieldContent.current_gbif),               # --> resource_id field
    ('gbif_publishingOrganizationKey', FieldContent.current_gbif), # GBIF organization UUID
    ('gbif_title', FieldContent.current_gbif),                  # --> resource field
    ('gbif_url', FieldContent.current_gbif),                    # --> resource_url field 
    ('gbif_description', FieldContent.current_gbif), 
    ('gbif_citation', FieldContent.current_gbif), 
    ('gbif_created', FieldContent.current_gbif), 
    ('gbif_modified', FieldContent.current_gbif), 
    # NEW merged table columns for BISON db
    ('bison_resource_uuid', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_resource_legacy_id', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_resource_name', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_resource_url', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_uuid', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_legacy_id', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_name', FieldContent.new_bison),                    # --> resource_url field 
    ('bison_provider_url', FieldContent.new_bison),                    # --> resource_url field 
)

# BISON provider data from solr core
BISON_PROVIDER_UNIQUE_COLS=('provider', 'resource_id', 'resource_url') 

# BISONPROVIDER values are column names in the bison.csv file provided by
# Denver developers for existing BISON provider data records.  These column
# names appear to come from postgres tables.  
# The mapping consists of an ordered list of tuples, where the first value 
# is the column name in bison.csv and the optional second value is the BISON 
# field name to match.  
#   * Tuples in the mapping with a single value in the tuple
#     have the same field name in both BISON and BISONPROVIDER.
#   * Tuples in the mapping with the 2nd value None, have no matching BISON fieldname
BISONPROVIDER_BISON_MAP = [
('dataset_key', None),
('catalogue_number', None),
('clean_provided_scientific_name', 'scientific_name'), 
('provided_scientific_name',),
('longitude', ), 
('latitude', 'latitude'), 
('iso_country_code', 'iso_country_code'),
('taxon_key', None), 
('year',),
('basis_of_record',),
('provider',), 
('provider_id',), 
('resource',), 
('resource_id',), 
('occurrence_date',),
('collector',), 
('county_original', 'provided_county_name'), 
('state_original', 'provided_state_name'), 
# Mismatch, postgres has computedcountyfips
('countycomputedfips', 'calculated_fips'),
('statecomputedfips', 'calculated_state_fips'),
('centroid',), 
('hierarchy_string', ), 
('tsns', 'valid_accepted_tsn'), 
('amb',), 
('provided_tsn', ), 
('collectornumber', 'collector_number'), 
('valid_accepted_scientific_name', ), 
# Duplicated above in tsns/valid_accepted_scientific_name
('valid_accepted_tsn',), 
('provided_common_name',), 
('itis_common_name',), 
('kingdom',), 
('itis_tsn',), 
('geodetic_datum',), 
('coordinate_precision',), 
('coordinate_uncertainty',), 
('verbatim_elevation',), 
('verbatim_depth',), 
('verbatim_locality',), 
('verbatim_event_date',), 
('provided_fips',), 
('calculated_county_name',), 
('calculated_state_name',), 
('provider_url',), 
('resource_url',), 
('occurrence_url',), 
('thumb_url',), 
('associated_media',), 
('associated_references',), 
('general_comments',), 
('bison_id', None), 
('license',), 
('calculated_mrgid', 'mrgid'), 
('calculated_waterbody',), 
('establishment_means',), 
('id', None), 
('tmpid', None)
]

