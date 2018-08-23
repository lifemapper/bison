
NAMESPACE = {'tdwg':   'http://rs.tdwg.org/dwc/text/',
             'gbif':   'http://rs.gbif.org/terms/1.0/',
             'eml':    'eml://ecoinformatics.org/eml-2.1.1',
             'xsi':    'http://www.w3.org/2001/XMLSchema-instance',
             'dublin': 'http://purl.org/dc/terms/'}
GBIF_URL = 'http://api.gbif.org/v1'
# http://api.gbif.org/v1/organization?identifier=362
BISON_UUID = 'c3ad790a-d426-4ac1-8e32-da61f81f0117'
ENCODING = 'utf-8'

# DATAPATH = '/tank/data/input/bison/'
DATAPATH = '/state/partition1/data/bison/'
SUBDIRS = ('terr', 'us')
DATASET_DIR = 'dataset'
META_FNAME = 'meta.xml'
# META_FNAME = '/state/partition1/data/bison/terr/meta.xml'
# META_FNAME = '/tank/data/input/bison/us/meta.xml'

CLIP_CHAR = '/'
DELIMITER = '\t'
NEWLINE = '\n'
URL_ESCAPES = [ [" ", "%20"], [",", "%2C"] ]

LOG_FORMAT = ' '.join(["%(asctime)s",
                       "%(threadName)s.%(module)s.%(funcName)s",
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
COMPUTED = None

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
PROHIBITED_VALS = ['na', '#na', 'n/a']

SAVE_FIELDS = {
   # pull canonical name from API and taxonKey
   'gbifID': (str, INTERPRETED),
   # pull canonical name from GBIF parser API and scientificName 
   #      or species API and taxonKey
   'scientificName': (str, INTERPRETED),
   'taxonKey': (str, INTERPRETED),
   'canonicalName': (str, COMPUTED),
   'vernacularName': (str, INTERPRETED), 
   'kingdom': (str, INTERPRETED), 
    
   'basisOfRecord': (str, INTERPRETED), 
   
   'eventDate': (str, INTERPRETED), 
   'year': (int, INTERPRETED),
   'verbatimEventDate': (str, INTERPRETED), 
   
   # GBIF 'Organization'
   #--------------
   'institutionCode': (str, INTERPRETED), 
   'institutionID': (str, INTERPRETED), 
#    # pull providerID from dataset API and publishingOrganizationKey
#    'publisher': (NO_OUTPUT, INTERPRETED),
   'providerID': (str, COMPUTED), 
   
   # GBIF 'Dataset'
   #--------------
   # I believe ownerInstitutionCode is incorrect, and should be collectionCode,
   # so including both.
   # pull resource from API and datasetKey
   'datasetKey': (NO_OUTPUT, INTERPRETED),
   'resourceID': (str, COMPUTED),
   'ownerInstitutionCode': (str, INTERPRETED),
   'collectionCode': (str, INTERPRETED),
   'collectionID': (str, INTERPRETED),

   # Occurrence record info
   'occurrenceID': (str, INTERPRETED),
   'catalogNumber': (str, INTERPRETED),
   'recordedBy': (str, INTERPRETED),
   'recordNumber': (str, INTERPRETED),
   
   # Geospatial occurrence record info
   'decimalLatitude': (float, INTERPRETED),
   'decimalLongitude': (float, INTERPRETED),
   'geodeticDatum': (str, INTERPRETED), 
   'coordinatePrecision': (str, INTERPRETED), 
   'coordinateAccuracy': (str, INTERPRETED), 
   'locality': (str, INTERPRETED), 
   'verbatimLocality': (str, INTERPRETED), 
   'habitat': (str, INTERPRETED), 
   'waterBody': (str, INTERPRETED), 
   'countryCode': (str, INTERPRETED), 
   # `elevation` is only in INTERPRETED, `verbatimElevation` in both
   'elevation': (str, INTERPRETED), 
   # `depth` is only in INTERPRETED, `verbatimDepth` in both
   'depth': (str, INTERPRETED), 
   'county': (str, INTERPRETED), 
   'higherGeographyID': (str, INTERPRETED), 
   'stateProvince': (str, INTERPRETED), 
         
   'license': (str, INTERPRETED), 
   # delete records with status=absent
   'occurrenceStatus': (str, INTERPRETED),
   # only in verbatim
#    'geodeticDatum': (str, INTERPRETED),
 }


ORDERED_OUT_FIELDS = [
   'gbifID', 'scientificName', 'taxonKey', 
#    'canonicalName',  
   'basisOfRecord', 'eventDate', 'year', 
   'verbatimEventDate', 'institutionCode', 'institutionID', 
   'ownerInstitutionCode', 'collectionCode', 'collectionID', 
   'occurrenceID', 'catalogNumber', 
   'recordedBy', 'recordNumber', 'decimalLatitude', 'decimalLongitude', 
   'elevation', 'depth', 'county', 'higherGeographyID', 'stateProvince', 
   # Keep original UUID values for later lookup
   'publisher',    # 'providerID', 
   'datasetKey',   # 'resourceID', 
   'vernacularName', 'kingdom', 
   'coordinatePrecision', 'locality', 'verbatimLocality','habitat', 'waterBody', 
   'countryCode', 'license']

TEST_FIELDS = ['occurrenceStatus', 'decimalLatitude', 'decimalLongitude']
COMPUTE_FIELDS = {'taxonKey': 'canonicalName', 
                  'publisher': 'providerID',
                  'datasetKey': 'resourceID',
                  'basisOfRecord': None}
