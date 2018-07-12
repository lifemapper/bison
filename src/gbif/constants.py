NAMESPACE = {'tdwg': 'http://rs.tdwg.org/dwc/text/',
             'gbif': 'http://rs.gbif.org/terms/1.0/',
             'dublin': 'http://purl.org/dc/terms/'}
GBIF_URL = 'http://api.gbif.org/v1'
ENCODING = 'utf-8'

DATAPATH = '/tank/data/input/bison/'
SUBDIRS = ('territories', 'us')
META_FNAME = '/tank/data/input/bison/us/meta.xml'

CLIP_CHAR = '/'
DELIMITER = '\t'

INTERPRETED = 'occurrence.txt'
VERBATIM = 'verbatim.txt'

NO_OUTPUT = None
COMPUTED = None

# Test these against lowercase values
PROHIBITED_VALS = ['na', '#na', 'n/a']

SAVE_FIELDS = {
   # pull canonical name from API and taxonKey
   'taxonKey': (NO_OUTPUT, INTERPRETED),
   'canonicalName': (str, COMPUTED),
    
   'basisOfRecord': (str, VERBATIM), 
   'eventDate': (str, VERBATIM), 
   'year': (int, VERBATIM),
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
   'elevation': (str, VERBATIM), 
   'depth': (str, VERBATIM), 
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

TEST_FIELDS = ['occurrenceStatus', 'decimalLatitude', 'decimalLongitude']
COMPUTE_FIELDS = {'taxonKey': 'canonicalName', 
                  'publisher': 'providerID',
                  'datasetKey': 'resourceID',
                  'basisOfRecord': None}
