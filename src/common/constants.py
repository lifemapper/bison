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
LEGACY_ID_DEFAULT = -9999

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

BISON_IPT_PREFIX = 'https://bison.usgs.gov/ipt/resource?r='
BISON_VALUES = {'provider': 'BISON',
                'provider_url': 'https://bison.usgs.gov',
                'provider_id': '440',
                'license' : 'http://creativecommons.org/publicdomain/zero/1.0/legalcode',
                }
ANCILLARY_DIR = 'ancillary'
ANCILLARY_FILES = {
    # Modified from original to merge US and Canada, split into simple
    # (no multi) polygons, and add centroids
    'terrestrial': {
        'file': 'us_can_boundaries_centroids.shp',
        'fields': (('B_FIPS', 'calculated_fips'), 
                   ('B_COUNTY', 'calculated_county_name'),
                   ('B_STATE', 'calculated_state_name'))},
    # Modified from original to split into simple polygons and 
    # intersected with a 5/10 degree grid
    'marine': {
#         'file': 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp',
#         'file': 'eez_gridded_boundaries_10.shp',
#         'file': 'eez_gridded_boundaries_5.shp',
        'file': 'eez_gridded_boundaries_2.5.shp',
        'fields': (('EEZ', 'calculated_waterbody'), 
                   ('MRGID', 'mrgid'))},
    # From Annie Simpson
    'establishment_means': {'file': 'NonNativesIndex20190912.txt'},
    # From ITIS developers
    'itis': {'file': 'itis_lookup.csv'},
    # From existing database
    'resource': {'file': 'resource.csv'},
    'provider': {'file': 'provider.csv'}}

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


BISON_ORDERED_DATALOAD_FIELDS = [
    'id',
    'clean_provided_scientific_name', 
    'itis_common_name',
    'itis_tsn',
    'hierarchy_string',                 # New field
    'amb',                              # New field
    'basis_of_record',
    'occurrence_date',
    'year',
    'verbatim_event_date',   
    'provider',
    'provider_url',    
    'resource',   
    'resource_url',   
    'occurrence_url',   
    'catalog_number',   
    'collector',
    'collector_number',  
    'valid_accepted_scientific_name',   
    'valid_accepted_tsn',   
    'provided_scientific_name',
    'provided_tsn',
    'latitude',
    'longitude',
    'verbatim_elevation',   
    'verbatim_depth',
    'calculated_county_name',   
    'calculated_fips',
    'calculated_state_name',
    'centroid',
    'provided_county_name', 
    'provided_fips',
    'provided_state_name',
    'thumb_url',
    'associated_media',
    'associated_references',
    'general_comments',
    'occurrence_id',                    # New field
    'provider_id',
    'resource_id',
    'provided_common_name',
    'kingdom',
    'geodetic_datum',
    'coordinate_precision',   
    'coordinate_uncertainty',
    'verbatim_locality',
    'mrgid',
    'calculated_waterbody',   
    'establishment_means',
    'iso_country_code',
    'license',
    ]

class FieldContent(Enum):
    legacy_bison = auto()
    current_gbif = auto()

MERGED_PROVIDER_LUT_FIELDS = (
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

    ('gbif_organizationKey', FieldContent.current_gbif),
    ('gbif_legacyid', FieldContent.current_gbif),               # --> provider_id field
    ('gbif_title', FieldContent.current_gbif),                  # --> record provider field
    ('gbif_url', FieldContent.current_gbif),                    # --> provider_url field 
    ('gbif_description', FieldContent.current_gbif), 
    ('gbif_citation', FieldContent.current_gbif), 
    ('gbif_created', FieldContent.current_gbif), 
    ('gbif_modified', FieldContent.current_gbif), 
)

MERGED_RESOURCE_LUT_FIELDS = (
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
    # Query with UUID for GBIF dataload
    ('dataset_id', FieldContent.legacy_bison),                  # GBIF dataset UUID
    ('owningorganization_id', FieldContent.legacy_bison),       # GBIF organization UUID
    ('provider_url', FieldContent.legacy_bison),
    ('provider_name', FieldContent.legacy_bison),

    ('gbif_datasetkey', FieldContent.current_gbif),             # GBIF dataset UUID
    ('gbif_legacyid', FieldContent.current_gbif),               # --> resource_id field
    ('gbif_publishingOrganizationKey', FieldContent.current_gbif), # GBIF organization UUID
    ('gbif_title', FieldContent.current_gbif),                  # --> resource field
    ('gbif_url', FieldContent.current_gbif),                    # --> resource_url field 
    ('gbif_description', FieldContent.current_gbif), 
    ('gbif_citation', FieldContent.current_gbif), 
    ('gbif_created', FieldContent.current_gbif), 
    ('gbif_modified', FieldContent.current_gbif), 
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
('statecomputedfips', None),
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

BISON_PROVIDER = {
    
    # my.usgs.gov/jira/browse/BISON-832
    '440,100012': 
    {'action': ProviderActions.replace, 
     'resource': 'USGS PWRC - Native Bee Inventory and Monitoring Lab (BIML)',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-biml',
     'filename': 'FINALusgspwrc-nativebeeinventoryandmonitoringlab-25Nov2019.txt',
    },
    
    # my.usgs.gov/jira/browse/BISON-402
    'nplichens':
    {'action': ProviderActions.add,
     'resource': 'NPS - US National Park Lichens - 2013 (NPLichens)',
     'resource_url': BISON_IPT_PREFIX + 'nplichens',
     'filename': 'FINAL-NPLichens03Dec2019.txt',
    },
    
    # my.usgs.gov/jira/browse/BISON-895
    'usgs-pwrc-amphibian-research-monitoring-initiative':
    {'action': ProviderActions.wait, # add,
     'resource': 'USGS PWRC - Amphibian Research and Monitoring Initiative (ARMI) - NEast Region',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-amphibian-research-monitoring-initiative',
     'filename': None
     },
    
    # my.usgs.gov/jira/browse/BISON-976
    'csu-nrel-co-riverbasin-tamarix':
    {'action': ProviderActions.add,
     'resource': 'CO State University - Natural Resource Ecology Lab - CO River Basin - Tamarix - 2014 and 2017',
     'resource_url': BISON_IPT_PREFIX + 'csu-nrel-co-riverbasin-tamarix',
     'filename': 'FINAL-CSU-NREL-CO-Riverbasin-Tamarix_20191126es.txt'
     },
    
    # my.usgs.gov/jira/browse/BISON-978
    '440,100061':
    {'action': ProviderActions.replace,
     'resource': 'BugGuide',
     'resource_url': BISON_IPT_PREFIX + 'bugguide',
     'filename': 'FINAL-bugguide19Dec2019.txt'},
    
    # my.usgs.gov/jira/browse/BISON-979
    '440,100068':
    {'action': ProviderActions.replace,
     'resource': 'Xerces Society - Bumble Bee Watch',
     'resource_url': BISON_IPT_PREFIX + 'xerces-bumblebeewatch',
     'filename': 'xerces-bumblebeewatch_FINAL14Aug2019.txt'},
    
    # my.usgs.gov/jira/browse/BISON-986
    '440,100071':
    {'action': ProviderActions.replace,
     'resource': 'Monarch Watch',
     'resource_url': BISON_IPT_PREFIX + 'monarchwatch',
     'filename': 'FINALmonarchwatch_2019Nov19.txt'},
    
    # my.usgs.gov/jira/browse/BISON-987
    '440,100028':
    {'action': ProviderActions.replace,
     'resource': 'USFS - Forest Inventory and Analysis - Trees (Public Lands)',
     'resource_url': BISON_IPT_PREFIX + 'usfs-fia-trees-public-lands',
     'filename': 'bison_fiapub_2019-03-25.txt'}, 
    
    # my.usgs.gov/jira/browse/BISON-988
    '440,100042':
    {'action': ProviderActions.replace,
     'resource': 'USFS - Forest Inventory and Analysis - Trees (Private Lands)',
     'resource_url': BISON_IPT_PREFIX + 'usfs-fia-trees-private-lands',
     'filename': 'bison_fiapriv_2019-03-25.txt'},
    
    # my.usgs.gov/jira/browse/BISON-989
    '440,100058':
    {'action': ProviderActions.replace_rename,
     'resource': 'New York City Tree Census - 2015',
     'resource_url': BISON_IPT_PREFIX + 'nycity-tree-census',
     'filename': 'FINAL-nycitytreecensus-2015-05Dec2019.txt'},
    
    # my.usgs.gov/jira/browse/BISON-993
    'usgs-warc-suwanneemoccasinshell':
    {'action': ProviderActions.add,
     'resource': 'USGS WARC - Florida and Georgia - Suwannee moccasinshell - 1916-2015',
     'resource_url': BISON_IPT_PREFIX + 'usgs-warc-suwanneemoccasinshell',
     'filename': 'revised_BISON_USGS_WARC_suwanneemoccasinshell.txt'},
    
    # my.usgs.gov/jira/browse/BISON-994
    '440,100046':
    {'action': ProviderActions.replace,
     'resource': 'USGS PWRC - Bird Banding Lab - US State Centroid',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-us-state',
     'filename': 'bison_bbl_state_ordered_final_2019-02-22.txt'},
    
    # my.usgs.gov/jira/browse/BISON-995
    '440,100008':
    {'action': ProviderActions.replace, 
     'resource': 'USGS PWRC - Bird Banding Lab - US 10min Block',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-us-10minblock',
     'filename': 'bison_bbl_10min_ordered_final_2019-02-21.txt'},
    
    # my.usgs.gov/jira/browse/BISON-996
    '440,100052':
    {'action': ProviderActions.replace,  
     'resource': 'USGS PWRC - Bird Banding Lab - Canada Province Centroid',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-canada-province',
     'filename': 'bison_bbl_ca_province_ordered_final_2019-02-26.txt'},
    
    # my.usgs.gov/jira/browse/BISON-998
    # No IPT url?
    '440,1066':
    {'action': ProviderActions.replace,
     'resource': 'USDA - PLANTS Database',
     'resource_url': 'http://plants.usda.gov/java/citePlants',
     'filename': 'bison_USDA_Plants_2019-12-19.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1001
    'nps-nisims':
    {'action': ProviderActions.add,
     'resource': 'NPS - National Invasive Species Information Management System - Plants',
     'resource_url': BISON_IPT_PREFIX + 'nps-nisims',
     'filename': 'NPS-NISIMS-Plants_20190325.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1002
    '440,100032':
    {'action': ProviderActions.replace_rename, 
     'resource': 'BLM - National Invasive Species Information Management System - Plants',
     'resource_url': BISON_IPT_PREFIX + 'blm_nisims',
     'filename': 'BLM-NISIMS_20190325.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1003
    '440,100060':
    {'action': ProviderActions.replace, 
     'resource': 'iMapInvasives - Plants - New York',
     'resource_url': BISON_IPT_PREFIX + 'imapinvasives-natureserve-ny',
     'filename': 'iMap_NY_plants-replace20190307.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1005
    '440,100043':
    {'action': ProviderActions.replace_rename, 
     'resource': 'USGS PWRC - North American Breeding Bird Survey',
     'resource_url': BISON_IPT_PREFIX + 'usgs_pwrc_north_american_breeding_bird_survey',
     'filename': 'bbsfifty_processed_2019-03-14.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1012
    'cdc-arbonet-mosquitoes':
    {'action': ProviderActions.add, 
     'resource': 'CDC - ArboNET - Mosquitoes',
     'resource_url': BISON_IPT_PREFIX + 'cdc-arbonet-mosquitoes',
     'filename': 'CDC-ArboNET-Mosquitoes_20190315.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1018
    'emammal':
    {'action': ProviderActions.add, 
     'resource': 'eMammal',
     'resource_url': BISON_IPT_PREFIX + 'emammal',
     'filename': 'bison_emammal_2019-09-18.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1051
    'usfws-waterfowl-survey':
    {'action': ProviderActions.add,
     'resource': '     USFWS - Waterfowl Breeding Population and Habitat Survey',
     'resource_url': BISON_IPT_PREFIX + 'usfws-waterfowl-survey',
     'filename': 'USFWS - Waterfowl Breeding Population and Habitat Survey 11Jul2019.txt'},
    
    # my.usgs.gov/jira/browse/BISON-1053
    # old name = USGS PIERC - Hawaii Forest Bird Survey Database 
    '440,100050':
    {'action': ProviderActions.rename, 
     'resource': 'USGS PIERC - Hawaii Forest Bird Survey - Plants',
     'resource_url': BISON_IPT_PREFIX + 'usgs_pierc_hfbs1',
     'filename': None},
    
    # my.usgs.gov/jira/browse/BISON-1075
    # old name = USGS PIERC - Hawaii Forest Bird Survey Database 
    '440,100075':
    {'action': ProviderActions.rename, 
     'resource': 'USGS PIERC - Hawaii Forest Bird Survey - Birds',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pierc-hfbs-birds',
     'filename': None},
    
#     # my.usgs.gov/jira/browse/BISON-
#     '':
#     {'action': ProviderActions.wait, 
#      'resource': '',
#      'resource_url': BISON_IPT_PREFIX + '',
#      'filename': None},
}
