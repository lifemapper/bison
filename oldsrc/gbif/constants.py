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
from common.constants import ALLOWED_TYPE

NAMESPACE = {'tdwg':   'http://rs.tdwg.org/dwc/text/',
             'gbif':   'http://rs.gbif.org/terms/1.0/',
             'eml':    'eml://ecoinformatics.org/eml-2.1.1',
             'xsi':    'http://www.w3.org/2001/XMLSchema-instance',
             'dublin': 'http://purl.org/dc/terms/'}


BISON_ORG_UUID = 'c3ad790a-d426-4ac1-8e32-da61f81f0117'

USDA_BEE_ORG_UUID = '1e26a630-7203-11dc-a0d8-b8a03c50a862'

GBIF_DATASET_PATHS = ['/tank/data/bison/2019/CA_USTerr_gbif/dataset',
                 '/tank/data/bison/2019/US_gbif/dataset']

# .............................................................................
class GBIF_ORG_KEYS(object):
    apitype = 'organization'
    # First 'save' key is organization UUID
    saveme = ['key', 'title', 'description', 'created', 'modified', 'homepage']
    preserve_format = ['description', 'homepage']

# .............................................................................
class GBIF_DSET_KEYS(object):
    """
    if Org key is BISON_ORG_UUID = 'c3ad790a-d426-4ac1-8e32-da61f81f0117'
    and identifiers/(one of numbered children with 
                     type=URL
                     identifier=BISON_IPT_PREFIX*
    """
    apitype = 'dataset'
    # First 'save' key is organization UUID
    saveme = ['key', 'publishingOrganizationKey', 'title', 'description', 
            'citation', 'rights', 'logoUrl', 'created', 'modified', 'homepage']
    preserve_format = ['title', 'rights', 'logoUrl', 'description', 'homepage']

    
    
GBIF_URL = 'http://api.gbif.org/v1'
GBIF_DATASET_URL = '{}/{}/'.format(GBIF_URL, GBIF_DSET_KEYS.apitype)
GBIF_ORGANIZATION_URL = '{}/{}/'.format(GBIF_URL, GBIF_ORG_KEYS.apitype)
GBIF_BATCH_PARSER_URL = GBIF_URL + '/parser/name/'
GBIF_SINGLE_PARSER_URL = GBIF_URL + '/species/parser/name?name='
GBIF_TAXON_URL = GBIF_URL + '/species/'
GBIF_UUID_KEY = 'key'
GBIF_ORG_FOREIGN_KEY = 'publishingOrganizationKey'

# http://api.gbif.org/v1/parser/name?name=quercus%20berberidifolia
# http://api.gbif.org/v1/organization?identifier=362
# http://api.gbif.org/v1/organization/c3ad790a-d426-4ac1-8e32-da61f81f0117

SUBDIRS = ('terr', 'us')
DATASET_DIR = 'dataset'
META_FNAME = 'meta.xml'

CLIP_CHAR = '/'
GBIF_DELIMITER = '\t'
GBIF_URL_ESCAPES = [ [" ", "%20"], [",", "%2C"] ]


INTERPRETED = 'occurrence'
VERBATIM = 'verbatim'
OUT_BISON = 'outBison'
CSV_EXT = '.csv'
SUBSET_PREFIX = '_lines_'

SUBSET = '0-5000'

NO_OUTPUT = None

TERM_CONVERT = {'FOSSIL_SPECIMEN': 'fossil',
                'LITERATURE': 'literature',
                'LIVING_SPECIMEN': 'living',
                'HUMAN_OBSERVATION': 'observation',
                'MACHINE_OBSERVATION': 'observation',
                'OBSERVATION': 'observation',
                'MATERIAL_SAMPLE': 'specimen',
                'PRESERVED_SPECIMEN': 'specimen',
                'UNKNOWN': 'unknown'}

class NS(object):
    dc = 'http://purl.org/dc/terms/'
    dwc = 'http://rs.tdwg.org/dwc/terms/'
    gbif = 'http://rs.gbif.org/terms/1.0/'

# gbifID mapped to BISON 'id'
OCC_ID_FLD = 'id'

GBIF_CONVERT_TEMP_FIELD_TYPE = {
    'occurrenceStatus': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
    'locality': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
    'habitat': 
     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
#      'ownerInstitutionCode':
#      {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
#      'collectionCode':
#      {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
#      'institutionID':
#      {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
#      'collectionID': 
#      {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
#     'datasetKey':
#     {'pgtype': ALLOWED_TYPE.varchar, 'max_len': None},
     }

GBIF_NAMEKEY_TEMP_FIELD = 'taxonKey'
GBIF_NAMEKEY_TEMP_TYPE = {'pgtype': ALLOWED_TYPE.integer}

# First pass mapping from GBIF data fields to BISON data fields plus a few 
# fields replaced in later computation
# Fields without a GBIF fieldname will be computed on a following pass
GBIF_TO_BISON2020_MAP = {
    # Temporary fields for calculations, discard before final output
    NS.gbif + 'taxonKey': 'taxonKey',
    NS.dwc + 'occurrenceStatus': 'occurrenceStatus',
    NS.dwc + 'locality': 'locality',
    NS.dwc + 'habitat': 'habitat',
    # Direct mapping, ties to original GBIF occ rec and dataset
    NS.gbif + 'gbifID': 'gbifID',
    # Direct mapping
    NS.dwc + 'basisOfRecord': 'basis_of_record',
    NS.dwc + 'eventDate': 'occurrence_date',
    NS.dwc + 'year': 'year',
    NS.dwc + 'verbatimEventDate': 'verbatim_event_date',
    NS.dwc + 'occurrenceID': 'occurrence_url',
    NS.dwc + 'catalogNumber': 'catalog_number', 
    NS.dwc + 'recordedBy': 'collector',
    NS.dwc + 'recordNumber': 'collector_number',
    NS.dwc + 'scientificName': 'provided_scientific_name',
    NS.dwc + 'taxonID': 'provided_tsn',
    NS.dwc +  'decimalLatitude': 'latitude',
    NS.dwc + 'decimalLongitude': 'longitude',
    NS.dwc + 'verbatimElevation' : 'verbatim_elevation', 
    NS.dwc + 'verbatimDepth': 'verbatim_depth',
    NS.dwc + 'county': 'provided_county_name', 
    NS.dwc + 'higherGeographyID': 'provided_fips',
    NS.dwc + 'stateProvince': 'provided_state_name',
    NS.dwc + 'associatedReferences': 'associated_references',
    NS.dwc + 'eventRemarks': 'general_comments',
    NS.dwc + 'occurrenceID': 'occurrence_id',
    NS.gbif + 'gbifID': 'id',
    NS.gbif + 'datasetKey': 'resource_id',
    NS.dwc + 'vernacularName': 'provided_common_name',
    NS.dwc + 'kingdom': 'kingdom',
    NS.dwc + 'coordinatePrecision': 'coordinate_precision',   
    NS.dwc + 'coordinateUncertaintyInMeters': 'coordinate_uncertainty',
    NS.dwc + 'verbatimLocality': 'verbatim_locality',
    NS.dwc + 'countryCode': 'iso_country_code',
    NS.dc + 'license': 'license'
    }

BISON_GBIF_MAP = {
    # Temporary fields for calculations, discard before final output
    'taxonKey': NS.gbif + 'taxonKey',
    'occurrenceStatus': NS.dwc + 'occurrenceStatus',
    'locality': NS.dwc + 'locality',
    'habitat': NS.dwc + 'habitat',
    # Direct mapping, ties to original GBIF occ rec and dataset
    'gbifID': NS.gbif + 'gbifID',
    # Direct mapping
    'basis_of_record': NS.dwc + 'basisOfRecord',
    'occurrence_date': NS.dwc + 'eventDate',
    'year': NS.dwc + 'year',
    'verbatim_event_date': NS.dwc + 'verbatimEventDate',
    'occurrence_url': NS.dwc + 'occurrenceID',
    'catalog_number': NS.dwc + 'catalogNumber', 
    'collector': NS.dwc + 'recordedBy',
    'collector_number': NS.dwc + 'recordNumber',  
    'provided_scientific_name': NS.dwc + 'scientificName',
    'provided_tsn': NS.dwc + 'taxonID',
    'latitude': NS.dwc +  'decimalLatitude',
    'longitude': NS.dwc + 'decimalLongitude',
    'verbatim_elevation': NS.dwc + 'verbatimElevation',   
    'verbatim_depth': NS.dwc + 'verbatimDepth',
    'provided_county_name': NS.dwc + 'county', 
    'provided_fips': NS.dwc + 'higherGeographyID',
    'provided_state_name': NS.dwc + 'stateProvince',
    'associated_references': NS.dwc + 'associatedReferences',
    'general_comments': NS.dwc + 'eventRemarks',
    'occurrence_id': NS.dwc + 'occurrenceID',
    'id': NS.gbif + 'gbifID',
    'resource_id': NS.gbif + 'datasetKey',
    'provided_common_name': NS.dwc + 'vernacularName',
    'kingdom': NS.dwc + 'kingdom',
    'coordinate_precision': NS.dwc + 'coordinatePrecision',   
    'coordinate_uncertainty': NS.dwc + 'coordinateUncertaintyInMeters',
    'verbatim_locality': NS.dwc + 'verbatimLocality',
    'iso_country_code': NS.dwc + 'countryCode',
    'license': NS.dc + 'license'
    }
