from enum import Enum, auto


NAMESPACE = {'tdwg':   'http://rs.tdwg.org/dwc/text/',
             'gbif':   'http://rs.gbif.org/terms/1.0/',
             'eml':    'eml://ecoinformatics.org/eml-2.1.1',
             'xsi':    'http://www.w3.org/2001/XMLSchema-instance',
             'dublin': 'http://purl.org/dc/terms/'}

# .............................................................................
class GBIF_ORG_KEYS(object):
    apitype = 'organization'
    # First 'save' key is organization UUID
    saveme = ['key', 'title', 'description', 'created', 'modified', 'homepage']
    preserve_format = ['description', 'homepage']

# .............................................................................
class GBIF_DSET_KEYS(object):
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
GBIF_ORG_UUID_FOREIGN_KEY = 'publishingOrganizationKey'

# http://api.gbif.org/v1/parser/name?name=quercus%20berberidifolia
# http://api.gbif.org/v1/organization?identifier=362
# http://api.gbif.org/v1/organization/c3ad790a-d426-4ac1-8e32-da61f81f0117
BISON_UUID = 'c3ad790a-d426-4ac1-8e32-da61f81f0117'
ENCODING = 'utf-8'

SUBDIRS = ('terr', 'us')
DATASET_DIR = 'dataset'
META_FNAME = 'meta.xml'

CLIP_CHAR = '/'
IN_DELIMITER = '\t'
OUT_DELIMITER = '$'
NEWLINE = '\n'
URL_ESCAPES = [ [" ", "%20"], [",", "%2C"] ]

LOG_FORMAT = ' '.join(["%(asctime)s",
                       "%(funcName)s",
                       "line",
                       "%(lineno)d",
                       "%(levelname)-8s",
                       "%(message)s"])

LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000 
LOGFILE_BACKUP_COUNT = 5


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


# Test these against lowercase values
PROHIBITED_VALS = ['na', '#na', 'n/a', '']

class NS(object):
    dc = 'http://purl.org/dc/terms/'
    dwc = 'http://rs.tdwg.org/dwc/terms/'
    gbif = 'http://rs.gbif.org/terms/1.0/'

LICENSE =  '(http://creativecommons.org/publicdomain/zero/1.0/legalcode) (DwC: license)'

class Method(Enum):
    calculate = auto()
    discard = auto()
    
OCC_UUID_FLD = 'gbifID'
TAX_UUID_FLD = 'taxonKey'
# 47 BISON fields - (+ gbifID and taxonKey)
#     only those with matching GBIF data from 
#         BISON DATA FIELDS (GDrive July 3.2018).xlsx
#     also enumerated in BISON DATA WORKFLOW(GDrive July 3.2018).pdf
BISON_GBIF_MAP = [
    # Discard this field before final output
    (OCC_UUID_FLD, OCC_UUID_FLD),
    # Discard this field before final output
    (TAX_UUID_FLD, TAX_UUID_FLD),
    # Discard this field before final output
    ('occurrenceStatus', 'occurrenceStatus'),
    ('clean_provided_scientific_name', Method.calculate),
    ('itis_common_name', Method.calculate),
    ('itis_tsn', Method.calculate),
    ('basis_of_record', NS.dwc + 'basisOfRecord'),
    ('occurrence_date', NS.dwc + 'eventDate'),
    ('year', NS.dwc + 'year'),
    ('verbatim_event_date', NS.dwc + 'verbatimEventDate'),   
    ('provider', Method.calculate), # ???(BISON) (DwC: institutionCode)
    ('provider_url', Method.calculate), # ??? (https://bison.usgs.gov)(DwC: institutionID)   
    ('resource', Method.calculate), # (dataset name) (DwC: collectionCode & datasetName)   
    ('resource_url', Method.calculate), # (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)   
    ('occurrence_url', NS.dwc + 'occurrenceID'), # (DwC: occurrenceID or IPT: occurrenceDetails)   
    ('catalog_number', NS.dwc + 'catalogNumber'),   
    ('collector', NS.dwc + 'recordedBy'),
    ('collector_number', NS.dwc + 'recordNumber'),  
    ('valid_accepted_scientific_name', Method.calculate),   
    ('acceptedNameUsage', Method.calculate),
    ('valid_accepted_tsn', Method.calculate),   
    ('provided_scientific_name', NS.dwc + 'scientificName'), #  taxonRemarks?
    ('provided_tsn', Method.calculate),
    ('latitude', NS.dwc +  'decimalLatitude'),
    ('longitude', NS.dwc + 'decimalLongitude'),
    ('verbatim_elevation', NS.dwc + 'verbatimElevation'),   
    ('verbatim_depth', NS.dwc + 'verbatimDepth'),
    ('calculated_county_name', Method.calculate),   
    ('calculated_fips', Method.calculate),
    ('calculated_state_name', Method.calculate),
    ('centroid', NS.dwc + 'georeferenceRemarks'),
    ('provided_county_name', NS.dwc + 'county'), 
    ('provided_fips', NS.dwc + 'higherGeographyID'),
    ('provided_state_name', NS.dwc + 'stateProvince'),
    ('thumb_url', Method.calculate),
    ('associated_media', NS.dwc + 'associatedMedia'),
    ('associated_references', NS.dwc + 'associatedReferences'),
    ('general_comments', NS.dwc + 'eventRemarks'),
    ('id', NS.dwc + 'occurrenceID'),
    ('provider_id', Method.calculate), # publishingOrganizationKey from dataset metadata
    ('resource_id', NS.gbif + 'datasetKey'),
    ('provided_common_name', NS.dwc + 'vernacularName'),
    ('kingdom', NS.dwc + 'kingdom'),
    ('geodetic_datum', NS.dwc + 'geodeticDatum'),
    ('coordinate_precision', NS.dwc + 'coordinatePrecision'),   
    ('coordinate_uncertainty',  NS.dwc + 'coordinateUncertaintyInMeters'),
    ('verbatim_locality', NS.dwc + 'verbatimLocality'),
    ('mrgid', Method.calculate),
    ('calculated_waterbody', Method.calculate),   
    ('establishment_means', Method.calculate),
    ('iso_country_code', NS.dwc + 'country'),
    ('license', Method.calculate)
    ]
