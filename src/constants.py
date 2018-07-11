ns = {'tdwg': 'http://rs.tdwg.org/dwc/text/'}
ENCODING = 'utf-8'

DATAPATH = '/tank/data/input/bison/'
SUBDIRS = ('territories', 'us')
META_FNAME = '/tank/data/input/bison/us/meta.xml'
CLIP_CHAR = '/'

INTERPRETED_BASENAME = 'occurrence.txt'
VERBATIM_BASENAME = 'verbatim.txt'
DELIMITER = '\t'

fieldmetaFname = '/tank/data/input/bison/us/meta.xml'


SAVE_FIELDS = {
   'canonicalName': (str, VERBATIM_BASENAME), 
   'basisOfRecord': (str, VERBATIM_BASENAME), 
   'eventDate': (str, VERBATIM_BASENAME), 
   'year': (int, VERBATIM_BASENAME),
   'verbatimEventDate': (str, VERBATIM_BASENAME), 
   'institutionCode': (str, VERBATIM_BASENAME), 
   'institutionId': (str, VERBATIM_BASENAME), 
   'ownerInstitutionCode': (str, VERBATIM_BASENAME),
   'collectionID': (str, VERBATIM_BASENAME),
   'occurrenceID': (str, VERBATIM_BASENAME),
   'catalogNumber': (str, VERBATIM_BASENAME),
   'recordedBy': (str, VERBATIM_BASENAME),
   'recordNumber': (str, VERBATIM_BASENAME),
   'decimalLatitude': (float, VERBATIM_BASENAME),
   'decimalLongitude': (float, VERBATIM_BASENAME),
   'elevation': (str, VERBATIM_BASENAME), 
   'depth': (str, VERBATIM_BASENAME), 
   'county': (str, VERBATIM_BASENAME), 
   'higherGeographyID': (str, VERBATIM_BASENAME), 
   'stateProvince': (str, VERBATIM_BASENAME), 
   'providerID': (str, VERBATIM_BASENAME), 
   'resourceID': (str, VERBATIM_BASENAME),
   'vernacularName': (str, VERBATIM_BASENAME), 
   'kingdom': (str, VERBATIM_BASENAME), 
   'geodeticDatum': (str, VERBATIM_BASENAME), 
   'coordinatePrecision': (str, VERBATIM_BASENAME), 
   'coordinateAccuracy': (str, VERBATIM_BASENAME), 
   'verbatimLocality': (str, VERBATIM_BASENAME), 
   'waterBody': (str, VERBATIM_BASENAME), 
   'countryCode': (str, VERBATIM_BASENAME), 
   'license': (str, VERBATIM_BASENAME), 
 }