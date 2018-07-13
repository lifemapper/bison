
NAMESPACE = {'tdwg':   'http://rs.tdwg.org/dwc/text/',
             'gbif':   'http://rs.gbif.org/terms/1.0/',
             'eml':    'eml://ecoinformatics.org/eml-2.1.1',
             'xsi':    'http://www.w3.org/2001/XMLSchema-instance',
             'dublin': 'http://purl.org/dc/terms/'}
GBIF_URL = 'http://api.gbif.org/v1'
ENCODING = 'utf-8'

DATAPATH = '/tank/data/input/bison/'
SUBDIRS = ('territories', 'us')
DATASET_DIR = 'dataset'
META_FNAME = '/tank/data/input/bison/us/meta.xml'

CLIP_CHAR = '/'
DELIMITER = '\t'

INTERPRETED = 'occurrence.txt'
VERBATIM = 'verbatim.txt'

NO_OUTPUT = None
COMPUTED = None

TERM_CONVERT = {'humanObservation': 'observation', 
                'machineObservation': 'observation',
                'preservedSpecimen': 'specimen', 
                'fossilSpecimen': 'specimen'}

# Test these against lowercase values
PROHIBITED_VALS = ['na', '#na', 'n/a']

SAVE_FIELDS = {
   # pull canonical name from API and taxonKey
   'gbifID': (str, INTERPRETED),
   # pull canonical name from API and taxonKey
   'taxonKey': (NO_OUTPUT, INTERPRETED),
   'canonicalName': (str, COMPUTED),
    
   'basisOfRecord': (str, VERBATIM), 
   'eventDate': (str, VERBATIM), 
   'year': (int, INTERPRETED),
   'verbatimEventDate': (str, VERBATIM), 
   'institutionCode': (str, VERBATIM), 
   'institutionId': (str, VERBATIM), 
   'ownerInstitutionCode': (str, VERBATIM),
   'collectionID': (str, VERBATIM),
   'occurrenceID': (str, VERBATIM),
   'catalogNumber': (str, VERBATIM),
   'recordedBy': (str, VERBATIM),
   'recordNumber': (str, VERBATIM),
   'decimalLatitude': (float, INTERPRETED),
   'decimalLongitude': (float, INTERPRETED),
   # `elevation` is only in INTERPRETED, `verbatimElevation` in both
   'elevation': (str, INTERPRETED), 
   # `depth` is only in INTERPRETED, `verbatimDepth` in both
   'depth': (str, INTERPRETED), 
   'county': (str, VERBATIM), 
   'higherGeographyID': (str, VERBATIM), 
   'stateProvince': (str, VERBATIM), 
   
   # pull publisher from API and ?
   'publisher': (NO_OUTPUT, INTERPRETED),
   'providerID': (str, COMPUTED), 
   
   # pull resource from API and datasetKey
   'datasetKey': (NO_OUTPUT, INTERPRETED),
   'resourceID': (str, COMPUTED),
   
   'vernacularName': (str, VERBATIM), 
   'kingdom': (str, VERBATIM), 
   'geodeticDatum': (str, VERBATIM), 
   'coordinatePrecision': (str, VERBATIM), 
   'coordinateAccuracy': (str, VERBATIM), 
   'verbatimLocality': (str, VERBATIM), 
   'waterBody': (str, VERBATIM), 
   'countryCode': (str, VERBATIM), 
   'license': (str, VERBATIM), 
   # delete records with status=absent
   'occurrenceStatus': (str, VERBATIM),
   'geodeticDatum': (str, VERBATIM),
 }

ORDERED_OUT_FIELDS = [
   'gbifID', 'canonicalName', 'basisOfRecord', 'eventDate', 'year', 
   'verbatimEventDate', 'institutionCode', 'institutionId', 
   'ownerInstitutionCode', 'collectionID', 'occurrenceID', 'catalogNumber', 
   'recordedBy', 'recordNumber', 'decimalLatitude', 'decimalLongitude', 
   'elevation', 'depth', 'county', 'higherGeographyID', 'stateProvince', 
   'providerID', 'resourceID', 'vernacularName', 'kingdom', 'geodeticDatum', 
   'coordinatePrecision', 'coordinateAccuracy', 'verbatimLocality', 'waterBody', 
   'countryCode', 'license', 'occurrenceStatus', 'geodeticDatum']

TEST_FIELDS = ['occurrenceStatus', 'decimalLatitude', 'decimalLongitude']
COMPUTE_FIELDS = {'taxonKey': 'canonicalName', 
                  'publisher': 'providerID',
                  'datasetKey': 'resourceID',
                  'basisOfRecord': None}
