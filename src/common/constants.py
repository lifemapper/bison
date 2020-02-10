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

BISON_VALUES = {'provider': 'BISON',
                'provider_url': 'https://bison.usgs.gov',
#                 'provider_id': '440',
                'license' : 'http://creativecommons.org/publicdomain/zero/1.0/legalcode',
                }

ANCILLARY_FILES = {'terrestrial': {'file': 'US_CA_Counties_Centroids.shp',
                                   'fields': (('FIPS', 'calculated_fips'), 
                                              ('COUNTY_NAM', 'calculated_county_name'),
                                              ('STATE_NAME', 'calculated_state_name'))},
                   'marine': {'file': 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp',
                              'fields': (('EEZ', 'calculated_waterbody'), 
                                         ('MRGID', 'mrgid'))},
                   'establishment_means': {'file': 'NonNativesIndex20190912.txt'},
                   'itis': {'file': 'itis_lookup.csv'}}

ISO_COUNTRY_CODES = ('AS', 'CA', 'FM', 'GU', 'MH', 'MP', 'PR', 'P', 'UM', 'US', 'VI')

class ProviderActions(Enum):
    replace = auto()
    add = auto()
    unknown = auto()

#     @staticmethod
#     def is_calc(mtype):
#         if mtype in (FillMethod.gbif_meta,
#                      FillMethod.gbif_name,
#                      FillMethod.itis_tsn, 
#                      FillMethod.georef, 
#                      FillMethod.est_means, 
#                      FillMethod.unknown):
#             return True
#         else:
#             return False

BISON_ORDERED_DATALOAD_FIELDS = [
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
    'id',
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
    '440,100012': 
    ('action': ProviderActions.replace, 
     'resource': 'USGS PWRC - Native Bee Inventory and Monitoring Lab (BIML)',
     'resource_url': 'https://bison.usgs.gov/ipt/resource?r=usgs-pwrc-biml',
     'filename': 'FINALusgspwrc-nativebeeinventoryandmonitoringlab-25Nov2019.txt'),
    '':
    ('action': ProviderActions.add,
     'resource': 'NPS - US National Park Lichens - 2013 (NPLichens)'
     'resource_url': 'https://bison.usgs.gov/ipt/resource?r=nplichens',
     'filename': 'FINAL-NPLichens03Dec2019.txt')
    }





