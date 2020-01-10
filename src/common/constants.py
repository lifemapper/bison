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


ENCODING = 'utf-8'
BISON_DELIMITER = '$'

NEWLINE = '\n'

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

ITIS_KINGDOMS = ('animalia', 'plantae', 'bacteria', 'fungi', 'protozofa', 
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
                'provider_id': '440',
                'license' : 'http://creativecommons.org/publicdomain/zero/1.0/legalcode',
                }

ANCILLARY_FILES = {'terrestrial': {'file': 'US_CA_Counties_Centroids.shp',
                                   'fields': (('FIPS', 'calculated_fips'), 
                                              ('COUNTY_NAM', 'calculated_county'),
                                              ('STATE_NAME', 'calculated_state'))},
                   'marine': {'file': 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp',
                              'fields': (('EEZ', 'calculated_waterbody'), 
                                         ('MRGID', 'mrgid'))},
                   'establishment_means': {'file': 'NonNativesIndex20190912.txt'},
                   'itis': {'file': 'itis_lookup.csv'}}

ISO_COUNTRY_CODES = ('AS', 'CA', 'FM', 'GU', 'MH', 'MP', 'PR', 'P', 'UM', 'US', 'VI')

class FillMethod(Enum):
    gbif_meta = auto()
    gbif_name = auto()
    itis_tsn = auto()
    georef = auto()
    est_means = auto()
    unknown = auto()

    @staticmethod
    def is_calc(mtype):
        if mtype in (FillMethod.gbif_meta,
                     FillMethod.gbif_name,
                     FillMethod.itis_tsn, 
                     FillMethod.georef, 
                     FillMethod.est_means, 
                     FillMethod.unknown):
            return True
        else:
            return False

    @staticmethod
    def in_pass1(mtype):
        if mtype in (FillMethod.gbif_name,
                     FillMethod.itis_tsn, 
                     FillMethod.georef, 
                     FillMethod.est_means, 
                     FillMethod.unknown):
            return False
        else:
            return True

    @classmethod
    def pass1(cls):
        return cls.gbif_meta
    
            
FillLater = (FillMethod.itis_tsn, FillMethod.georef, FillMethod.est_means, 
             FillMethod.unknown)

# BISON provider data from solr core
BISON_PROVIDER_UNIQUE_COLS=('provider', 'resource_id', 'resource_url') 
BISON_PROVIDER_HEADER = (
'dataset_key', 
'catalogue_number', 
'scientific_name', 
'provided_scientific_name', 
'longitude', 
'latitude', 
'iso_country_code', 
'taxon_key', 
'year', 
'basis_of_record', 
'provider', 
'provider_id', 
'resource', 
'resource_id', 
'occurrence_date', 
'collector', 
'county_original', 
'state_original', 
'countycomputedfips', 
'statecomputedfips', 
'centroid', 
'hierarchy_string', 
'tsns', 
'amb', 
'provided_tsn', 
'collectornumber', 
'valid_accepted_scientific_name', 
'valid_accepted_tsn', 
'provided_common_name', 
'itis_common_name', 
'kingdom', 'itis_tsn', 
'geodetic_datum', 
'coordinate_precision', 
'coordinate_uncertainty', 
'verbatim_elevation', 
'verbatim_depth', 
'verbatim_locality', 
'verbatim_event_date', 
'provided_fips', 
'calculated_county_name', 
'calculated_state_name', 
'provider_url', 
'resource_url', 
'occurrence_url', 
'thumb_url', 
'associated_media', 
'associated_references', 
'general_comments', 
'bison_id', 
'license', 
'calculated_mrgid', 
'calculated_waterbody', 
'establishment_means', 
'id', 
'tmpid'
)






