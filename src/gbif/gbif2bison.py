"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import csv
import codecs
import os
import sys
import StringIO
import xml.etree.ElementTree as ET

from constants import *
from gbifresolve import GBIFCodes

# .............................................................................
class UTF8Recoder:
   """
   Iterator that reads an encoded stream and reencodes the input to UTF-8
   """
   def __init__(self, f, encoding):
      self.reader = codecs.getreader(encoding)(f)

   def __iter__(self):
      return self

   def next(self):
      return self.reader.next().encode("utf-8")
     
# .............................................................................
class UnicodeReader:
   """
   A CSV reader which will iterate over lines in the CSV file "f",
   which is encoded in the given encoding.
   """

   def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
      f = UTF8Recoder(f, encoding)
      self.reader = csv.reader(f, dialect=dialect, **kwds)

   def next(self):
      row = self.reader.next()
      return [unicode(s, "utf-8") for s in row]

   def __iter__(self):
      return self


# .............................................................................
class GBIFReader(object):
   """
   @summary: GBIF Record containing CSV record of 
    * original provider data from verbatim.txt
    * GBIF-interpreted data from occurrence.txt
   """
   # ...........................
   def __init__(self, verbatimFname, interpretedFname, metaFname):
      """
      @summary: Constructor
      """
      self.codeResolver = GBIFCodes()
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
   def open(self):
      self.fldMeta = self.getFieldMeta()
      (self.intCsvreader, 
       self._intF) = self.getCSVReader(self.interpFname, DELIMITER)
      (self.verbCsvreader, 
       self._verbF)  = self.getCSVReader(self.verbatimFname, DELIMITER)
      # Pull the header row from each file, we use metadata for field indices
      _ = self.getLine(self.verbCsvreader)
      _ = self.getLine(self.intCsvreader)
   
   # .............................................................................
   def isOpen(self):
      """
      @note: returns True if either is open
      """
      if ((not self._intF is None and not self._intF.closed) or 
          (not self._verbF is None and self._verbF.closed)):
         return True
      return False

   # .............................................................................
   def close(self):
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
               idx = int(fld.get('index'))
               temp = fld.get('term')
               term = temp[temp.rfind(CLIP_CHAR)+1:]
               if term in SAVE_FIELDS.keys():
                  if not fields.has_key(term):
                     fields[term] = {currmeta: idx,
                                     'dtype': SAVE_FIELDS[term][0],
                                     'version': SAVE_FIELDS[term][1]}
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
   def _getCanonicalName(self, val):
      canName = self.codeResolver.resolveTaxonKey(val)
      return canName

   # ...............................................
   def _getProviderID(self, val):
      canName = self.codeResolver.resolveTaxonKey(val)
      return canName

   # ...............................................
   def _getResourceID(self, val):
      return val
   
   # ...............................................
   def _testOccurrenceStatus(self, val):
      if val.lower() == 'absent':
         return None
      return val

   # ...............................................
   def _getPoint(self, lon, lat, cntry):
      if lon == 0 and lat == 0:
         lon = None
         lat = None
      elif cntry == 'US' and lon is not None and lon > 0:
         lon = lon * -1 
      return lon, lat

   # ...............................................
   def _chuckIt(self, rec):
      pass

   # ...............................................
   def _cleanVal(self, val):
      val = val.strip()
      if val.lower() in PROHIBITED_VALS:
         val = ''
      return val
   # ...............................................
   def createBisonData(self, verbline, intline):
      """
      @param verbLine: A CSV record of original provider DarwinCore occurrence data 
      @param intLine: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
      rec = {}
      self.fldMeta.keys()
      for fldname, meta in self.fldMeta:
         if meta['version'] == VERBATIM:
            val = verbline[meta[VERBATIM]]
         else:
            val = intline[meta[INTERPRETED]]
         # Update fields to be edited
         # delete records with no canonical name
         if fldname == 'taxonKey':
            val = self._getCanonicalName(val)
            if val is None:
               rec = None
               break
         elif fldname == 'countryCode':
            cntry = val
         elif fldname == 'decimalLongitude':
            lon = val
         elif fldname == 'decimalLatitude':
            lat = val
         # delete absence records
         elif fldname == 'occurrenceStatus':
            if val.lower() == 'absent':
               rec = None
               break
         
         lon, lat = self._getPoint(lon, lat, cntry)
         rec[fldname] = val
      return rec
   
   # ...............................................
   def extractData(self):      
      try:
         if self.isOpen():
            self.close()
         verbCsvreader, intCsvreader = self.open()
         
         while (self._recnum < 4 and 
                verbCsvreader is not None and 
                intCsvreader is not None):
            verbline = self.getLine(self.verbCsvreader)
            intline = self.getLine(self.intCsvreader)
            byline = self.createBisonLine(verbline, intline)
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
gr.open()

vcsv = gr.verbCsvreader
icsv = gr.intCsvreader

verbLine = gr.getLine(gr.verbCsvreader)
intLine = gr.getLine(gr.intCsvreader)


currmeta = fnameElt.text



gr.extractData()

"""
   
   
   
         