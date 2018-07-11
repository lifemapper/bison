import os
import csv
import xml.etree.ElementTree as ET
import sys
import StringIO

from constants import *
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
class GBIFResolver(object):
   """
   @summary: GBIF Record containing CSV record of 
    * original provider data from verbatim.txt
    * GBIF-interpreted data from occurrence.txt
   """
   # ...........................
   def __init__(self, verbatimFname, interpretedFname, metaFname):
      """
      @summary: Constructor
      @param gbifId: GBIF record id for the CSV data lines
      @param provLine: A CSV record of original provider DarwinCore occurrence data 
      @param gbifLine: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      @param metadict: A dictionary of field names, indexes, types for both 
                       original and interpreted data
      """
      self.verbatimFname = verbatimFname
      self.interpFname = interpretedFname
      self._intF = None
      self._verbF = None
      self._intCsvreader = None
      self._verbCsvreader = None
      self.metaFname = metaFname
      self.fldMeta = None
      self._recnum = 0
      
   # .............................................................................
   def _open(self):
      self.fldMeta = self.getFieldMeta()
      self._intF = open(self.interpFname, 'r')
      self._verbF = open(self.verbatimFname, 'r')
      intCsvreader = self.getCSVReader(self._intF, DELIMITER)
      verbCsvreader = self.getCSVReader(self._verbF, DELIMITER)
      return verbCsvreader, intCsvreader
   
   # .............................................................................
   def _close(self):
      self._intF.close()
      self._verbF.close()

   # .............................................................................
   def getFieldMeta(self):
      '''
      @summary: Read metadata for verbatim and interpreted data files, and
                for fields we are interested in:
                extract column index for each file, add datatype. and 
                add which file (verbatim or interpreted) to extract field value  
                from.  Resulting metadata will look like:
                     fields = {term: {VERBATIM_BASENAME: columnIndex,
                                      INTERPRETED_BASENAME: columnIndex, 
                                      dtype: str,
                                      useVersion: ?_BASENAME}, ...}
      '''
      tdwg = '{http://rs.tdwg.org/dwc/text/}'
      fields = {}
      tree = ET.parse(META_FNAME)
      root = tree.getroot()
      # Child will reference INTERPRETED or VERBATIM file
      for child in root:
         fileElt = child.find('tdwg:files', NAMESPACE)
         fnameElt= fileElt .find('tdwg:location', NAMESPACE)
         currmeta = fnameElt.text
         if currmeta in (INTERPRETED, VERBATIM):
            flds = child.findall(tdwg+'field')
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

   # .............................................................................
   def getCSVReader(self, datafile, delimiter):
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
   def getLine(self, csvreader):
      success = False
      line = None
      while not success and csvreader is not None:
         self._recnum += 1
         try:
            line = csvreader.next()
            success = True
         except OverflowError, e:
            print( 'Overflow on line {} ({})'.format(self._recnum, str(e)))
         except StopIteration:
            print('EOF on line {}'.format(self._recnum))
            success = True
         except Exception, e:
            print('Bad record on line {} ({})'.format(self._recnum, e))
         
      return line
   
   # ...............................................
   def extractData(self):      
      try:
         verbCsvreader, intCsvreader = self._open()
         
         while (self._recnum < 4 and 
                verbCsvreader is not None and 
                intCsvreader is not None):
            verbLine = self.getLine(verbCsvreader)
            intLine = self.getLine(intCsvreader)
   
      finally:
         self._close()
         
# ...............................................
if __name__ == '__main__':
   subdir = SUBDIRS[0]
   interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
   verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
   gr = GBIFResolver(verbatimFname, interpFname)
   gr.extractData()
   
   
"""
import os
import csv
import xml.etree.ElementTree as ET
import sys
import StringIO

from gbif.constants import *
from gbif.portaldownload import *

subdir = SUBDIRS[0]
interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
gr = GBIFResolver(verbatimFname, interpFname, META_FNAME)
gr._open()



currmeta = fnameElt.text



gr.extractData()

"""
   
   
   
         