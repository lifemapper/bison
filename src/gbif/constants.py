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

from common.constants import FillMethod


NAMESPACE = {'tdwg':   'http://rs.tdwg.org/dwc/text/',
             'gbif':   'http://rs.gbif.org/terms/1.0/',
             'eml':    'eml://ecoinformatics.org/eml-2.1.1',
             'xsi':    'http://www.w3.org/2001/XMLSchema-instance',
             'dublin': 'http://purl.org/dc/terms/'}


BISON_ORG_UUID = 'c3ad790a-d426-4ac1-8e32-da61f81f0117'
USDA_BEE_ORG_UUID = '1e26a630-7203-11dc-a0d8-b8a03c50a862'
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
                     identifier=https://bison.usgs.gov/ipt/resource?r=*
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
# COMPUTED = None

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

    
OCC_UUID_FLD = 'gbifID'

DISCARD_FIELDS = ['occurrenceStatus', 'locality', 'habitat']
DISCARD_AFTER_UPDATE = [OCC_UUID_FLD, 'taxonKey']
# 47 BISON fields - (+ gbifID and taxonKey)
#     only those with matching GBIF data from 
#         BISON DATA FIELDS (GDrive July 3.2018).xlsx
#     also enumerated in BISON DATA WORKFLOW(GDrive July 3.2018).pdf
BISON_GBIF_MAP = [
    # Save UUID field for link back to original data
    (OCC_UUID_FLD, OCC_UUID_FLD),
    # Discard these fields before final output
    ('taxonKey', 'taxonKey'),
    ('occurrenceStatus', 'occurrenceStatus'),
    ('locality', 'locality'),
    ('habitat', 'habitat'),
    # Save fields start here
    ('clean_provided_scientific_name', FillMethod.gbif_name),
    ('itis_common_name', FillMethod.itis_tsn),
    ('itis_tsn', FillMethod.itis_tsn),
    ('basis_of_record', NS.dwc + 'basisOfRecord'),
    ('occurrence_date', NS.dwc + 'eventDate'),
    ('year', NS.dwc + 'year'),
    ('verbatim_event_date', NS.dwc + 'verbatimEventDate'),   
#     ('provider', FillMethod.gbif_meta),
    ('provider', NS.dwc + 'institutionCode'),
#     ('provider_url', FillMethod.gbif_meta),   
    ('provider_url', NS.dwc + 'institutionID'),    
    ('resource', FillMethod.gbif_meta), # (dataset name) (DwC: collectionCode & datasetName)   
    ('resource_url', FillMethod.gbif_meta), # (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)   
    ('occurrence_url', NS.dwc + 'occurrenceID'), # (DwC: occurrenceID or IPT: occurrenceDetails)   
    ('catalog_number', NS.dwc + 'catalogNumber'),   
    ('collector', NS.dwc + 'recordedBy'),
    ('collector_number', NS.dwc + 'recordNumber'),  
    ('valid_accepted_scientific_name', FillMethod.itis_tsn),   
    ('valid_accepted_tsn', FillMethod.itis_tsn),   
    ('provided_scientific_name', NS.dwc + 'scientificName'), #  taxonRemarks?
    ('provided_tsn', NS.dwc + 'taxonID'),
    ('latitude', NS.dwc +  'decimalLatitude'),
    ('longitude', NS.dwc + 'decimalLongitude'),
    ('verbatim_elevation', NS.dwc + 'verbatimElevation'),   
    ('verbatim_depth', NS.dwc + 'verbatimDepth'),
    ('calculated_county_name', FillMethod.georef),   
    ('calculated_fips', FillMethod.georef),
    ('calculated_state_name', FillMethod.georef),
    ('centroid', FillMethod.georef),
    ('provided_county_name', NS.dwc + 'county'), 
    ('provided_fips', NS.dwc + 'higherGeographyID'),
    ('provided_state_name', NS.dwc + 'stateProvince'),
    ('thumb_url', FillMethod.unknown),
    ('associated_media', FillMethod.unknown), # dwc associatedMedia from verbatim file
    ('associated_references', NS.dwc + 'associatedReferences'),
    ('general_comments', NS.dwc + 'eventRemarks'),
    ('id', NS.dwc + 'occurrenceID'),
    ('provider_id', FillMethod.gbif_meta), # publishingOrganizationKey from dataset metadata
    ('resource_id', NS.gbif + 'datasetKey'),
    ('provided_common_name', NS.dwc + 'vernacularName'),
    ('kingdom', NS.dwc + 'kingdom'),
    ('geodetic_datum', FillMethod.unknown), # dwc geodeticDatum from verbatim file
    ('coordinate_precision', NS.dwc + 'coordinatePrecision'),   
    ('coordinate_uncertainty',  NS.dwc + 'coordinateUncertaintyInMeters'),
    ('verbatim_locality', NS.dwc + 'verbatimLocality'),
    ('mrgid', FillMethod.georef),
    ('calculated_waterbody', FillMethod.georef),   
    ('establishment_means', FillMethod.est_means),
    ('iso_country_code', NS.dwc + 'countryCode'),
    ('license', NS.dc + 'license') # or constant?
    ]
