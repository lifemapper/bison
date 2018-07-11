import os
import xml
import json
import csv
import xml.etree.ElementTree as ET
import sys
import StringIO

from constants import (ns, DATAPATH, SUBDIRS, META_FNAME, ENCODING,
                       INTERPRETED_BASENAME, VERBATIM_BASENAME,
                       DELIMITER, CLIP_CHAR, SAVE_FIELDS)

LINENO = 0
# .............................................................................
def getFileMeta():
   '''
   @summary: Read metadata for verbatim and interpreted data files, and
             for fields we are interested in:
             extract column index for each file, and
             add datatype and 
             add which file (verbatim or interpreted) 
             to save data from.  Resulting metadata will look like:
                  fields = {term: {VERBATIM_BASENAME: columnIndex,
                                   INTERPRETED_BASENAME: columnIndex, 
                                   dtype: str,
                                   useVersion: ?_BASENAME}}
   '''

   fields = {}
   tree = ET.parse(META_FNAME)
   root = tree.getroot()
   # Child will reference INTERPRETED or VERBATIM file
   for child in root:
      fls = child.find('tdwg:files', ns)
      dloc = fls.find('tdwg:location', ns)
      currmeta = dloc.text
      if currmeta in (INTERPRETED_BASENAME, VERBATIM_BASENAME): 
         flds = child.findall('tdwg:field')
         for fld in flds:
            idx = fld.get('index')
            temp = fld.get('term')
            term = temp[temp.rfind(CLIP_CHAR)+1:]
            if term in SAVE_FIELDS.keys():
               if not fields.has_key(term):
                  fields[term] = {currmeta: idx,
                                  'dtype': SAVE_FIELDS[term][0],
                                  'useVersion': SAVE_FIELDS[term][1]}
               else:
                  fields[term][currmeta] = idx
   return fields
 
'''
Initial extract of requested GBIF data fields based on our 11 iso_country_codes e.g. AS,CA, FM, GU, MH, MP, PR, PW, UM, US, and VI
 (see Pg 20 of BISON Data Workflow (July 3. 2018).pdf)
Eliminate all records from the BISON Provider (440) (to avoid duplication in BISON)
Run scientificName values through either the Python script (attached pyGbifLoad.zip) OR GBIF name parser (https://www.gbif.org/tools/name-parser) and replace with resulting canonicalName values (we want the original clean scientific name submitted by the Data Providers sans taxon author and year; rather than GBIF's 'interpreted' scientificName values)
Remove all records with scientificName (or canonicalName?)=blank
Research/remove all records with occurrenceStatus=absent (if available in GBIF download)
For records with 0,0 coordinates - change any decimalLatitude and decimalLongitude field '0' (zero) values to null/blank (we/BISON may still be able to georeference these records)
Convert GBIF basisOfRecord (http://rs.tdwg.org/dwc/terms/#basisOfRecord) values to simpler BISON values e.g. 
humanObservation and machineObservation=observation; FossilSpecimen=fossil, LivingSpecimen=living;... 

'''     
# .............................................................................
# .............................................................................
class GBIFRec(object):
   """
   @summary: GBIF Record containing CSV record of 
    * original provider data from verbatim.txt
    * GBIF-interpreted data from occurrence.txt
   """
   # ...........................
   def __init__(self, provLine, gbifLine, metaDict):
      """
      @summary: Constructor
      @param gbifId: GBIF record id for the CSV data lines
      @param provLine: A CSV record of original provider DarwinCore occurrence data 
      @param gbifLine: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      @param metadict: A dictionary of field names, indexes, types for both 
                       original and interpreted data
      """
      pass
  
# .............................................................................
# .............................................................................
def getCSVReader(datafile, delimiter):
   f = None  
   csv.field_size_limit(sys.maxsize)
   try:
      f = open(datafile, 'r')
      csvreader = csv.reader(f, delimiter=delimiter)
   except Exception, e:
      try:
         f = StringIO.StringIO()
         f.write(datafile.encode(ENCODING))
         f.seek(0)
         csvreader = csv.reader(f, delimiter=delimiter)
      except IOError, e:
         raise 
      except Exception, e:
         raise Exception('Failed to read or open {}, ({})'
                         .format(datafile, str(e)))
   return csvreader, f

# ...............................................
def getLine(csvreader):
   success = False
   line = None
   while not success and csvreader is not None:
      try:
         LINENO += 1
         line = csvreader.next()
         success = True
      except OverflowError, e:
         print( 'Overflow ({})'.format(str(e)))
      except StopIteration:
         print('EOF')
         success = True
      except Exception, e:
         print('Bad record {}'.format(e))
      
   return line

# ...............................................
def extractData():
   subdir = SUBDIRS[0]
   interpFname = os.path.join(DATAPATH, subdir, INTERPRETED_BASENAME)
   verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM_BASENAME)
   
   try:
      intF = open(interpFname, 'r')
      verbF = open(verbatimFname, 'r')
      intRdr = getCSVReader(intF, DELIMITER)
      verbRdr = getCSVReader(verbF, DELIMITER)
      fieldMeta = getFileMeta()
      
      while LINENO < 4 and verbRdr is not None and intRdr is not None:
         verbLine = getLine(verbRdr)
         intLine = getLine(intRdr)

   finally:
      intF.close()
      verbF.close()
      