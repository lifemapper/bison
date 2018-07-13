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
from gbifapi import GBIFCodes

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
   def __init__(self, verbatimFname, interpretedFname, metaFname, outFname, datasetPath):
      """
      @summary: Constructor
      """
      self.codeResolver = GBIFCodes()
      self.verbatimFname = verbatimFname
      self.interpFname = interpretedFname
      self.outFname = outFname
      self._datasetPath = datasetPath
      self._if = None
      self._vf = None
      self._iCsvreader = None
      self._vCsvreader = None
      self._metaFname = metaFname
      self.fldMeta = None
      self.dsMeta = {}
      
   # .............................................................................
   def open(self):
      '''
      @summary: Read metadata and open datafiles for reading
      '''
      self.fldMeta = self.getFieldMeta()
      (self._iCsvreader, 
       self._if) = self.getCSVReader(self.interpFname, DELIMITER)
      (self._vCsvreader, 
       self._vf)  = self.getCSVReader(self.verbatimFname, DELIMITER)
      # Pull the header row from each file, we use metadata for field indices
      _ = self.getLine(self._vCsvreader, 0)
      _ = self.getLine(self._iCsvreader, 0)
   
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
      '''
      @summary: Close input datafiles
      '''
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
      tree = ET.parse(self._metaFname)
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
   def getDatasetMeta(self, uuid):
      '''
      @summary: Read metadata for dataset with this uuid
      dataset element contains:
         alternateIdentifier
         alternateIdentifier
         alternateIdentifier
         title
         creator
         metadataProvider
         associatedParty
         pubDate
         language
         abstract
         keywordSet
         keywordSet
         intellectualRights
         distribution
         coverage
         maintenance
         contact
         methods
         project
      '''
      fname = os.path.join(self._datasetPath, '{}.txt'.format(uuid))
      tree = ET.parse(fname)
      root = tree.getroot()
      ds  = root.find('dataset')
      title = ds.find('title').text
      return title

   # .............................................................................
   def getCSVReader(self, datafile, delimiter):
      '''
      @summary: Get a CSV reader that can handle encoding
      '''
      f = None  
      csv.field_size_limit(sys.maxsize)
      try:
         f = codecs.open(datafile, 'r', ENCODING)
#          f = open(datafile, 'r')
         csvreader = csv.reader(f, delimiter=delimiter)
      except Exception, e:
#          try:
#             f = StringIO.StringIO()
#             f.write(datafile.encode(ENCODING))
#             f.seek(0)
#             csvreader = csv.reader(f, delimiter=delimiter)
#          except IOError, e:
#             raise 
#          except Exception, e:
         raise Exception('Failed to read or open {}, ({})'
                         .format(datafile, str(e)))
      return csvreader, f
   
   # .............................................................................
   def getCSVDictWriter(self, datafile, fldnames, delimiter):
      '''
      @summary: Get a CSV writer that can handle encoding
      '''
      f = None  
      csv.field_size_limit(sys.maxsize)
      try:
         f = codecs.open(datafile, 'w', ENCODING)
         csvreader = csv.writer(f, delimiter=delimiter)
      except Exception, e:
         raise Exception('Failed to read or open {}, ({})'
                         .format(datafile, str(e)))
      return csvreader, f

   # ...............................................
   def getLine(self, csvreader, recno):
      '''
      @summary: Return a line while keeping track of the line number and errors
      '''
      success = False
      line = None
      while not success and csvreader is not None:
         recno += 1
         try:
            line = csvreader.next()
            success = True
         except OverflowError, e:
            print( 'Overflow on line {} ({})'.format(recno, str(e)))
         except StopIteration:
            print('EOF on line {}'.format(recno))
            success = True
         except Exception, e:
            print('Bad record on line {} ({})'.format(recno, e))
         
      return line, recno

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
   def createBisonData(self, vline, iline):
      """
      @param verbLine: A CSV record of original provider DarwinCore occurrence data 
      @param intLine: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
      rec = {}
      for fldname, meta in self.fldMeta.iteritems():
         if meta['version'] == VERBATIM:
            try:
               val = vline[self.fldMeta[fldname][VERBATIM]]
            except KeyError, e:
               print'{} not in VERBATIM data, using INTERPRETED'.format(fldname) 
               val = iline[self.fldMeta[fldname][INTERPRETED]]
         else:
            try:
               val = iline[self.fldMeta[fldname][INTERPRETED]]
            except KeyError, e:
               print'{} not in INTERPRETED data, using VERBATIM'.format(fldname) 
               val = vline[self.fldMeta[fldname][VERBATIM]]

         # Replace N/A
         if val.lower() in PROHIBITED_VALS:
            val = ''
            print'Field {}, val {} prohibited'.format(fldname, val) 
         # delete records with no canonical name
         if fldname == 'taxonKey':
            val = self._getCanonicalName(val)
            if val is None:
               rec = None
               print'Field {}, val {} does not contain canonical'.format(fldname, val) 
               break
            else:
               rec['canonicalName'] = val
         # delete absence records
         elif fldname == 'occurrenceStatus':
            if val.lower() == 'absent':
               rec = None
               print'Field {}, val {} is absence data'.format(fldname, val) 
               break
         # simplify basisOfRecord terms
         elif fldname == 'basisOfRecord':
            if val in TERM_CONVERT.keys():
               val = TERM_CONVERT[val]
               print'Field {}, val {} converted'.format(fldname, val) 
         # Convert year to integer
         elif fldname == 'year':
            try:
               val = int(val)
            except:
               print'Field {}, val {} is not an integer'.format(fldname, val) 
               val = None
         # check Long/Lat fields
         elif fldname == 'countryCode':
            cntry = val
         elif fldname == 'decimalLongitude':
            lon = float(val)
         elif fldname == 'decimalLatitude':
            lat = float(val)
         # Save modified val
         if fldname not in ('decimalLongitude', 'decimalLatitude'):
            rec[fldname] = val
         
      lon, lat = self._getPoint(lon, lat, cntry)
      rec['decimalLongitude'] = lon
      rec['decimalLatitude'] = lat
      return rec
   
   # ...............................................
   def extractData(self):      
      try:
         if self.isOpen():
            self.close()
         self.open()
         vRecno = iRecno = 1
         
         outWriter, outf = self.getCSVDictWriter(self.outFname, 
                                                 ORDERED_OUT_FIELDS, 
                                                 DELIMITER)
         outWriter.writerheader()
         while (self._vCsvreader is not None and 
                self._iCsvreader is not None):
            vline, vRecno = self.getLine(self.verbCsvreader, vRecno)
            iline, iRecno = self.getLine(self.intCsvreader, iRecno)
            byline = self.createBisonLine(vline, iline)
            outWriter.writerow(byline)
      finally:
         self._close()
         outf.close()
         
# ...............................................
if __name__ == '__main__':
   subdir = SUBDIRS[0]
   interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
   verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
   outFname = os.path.join(DATAPATH, subdir, 'outBison.csv')
   datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR) 
   gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)
   gr.extractData()
   
   
"""
import csv
import codecs
import os
import sys
import StringIO
import xml.etree.ElementTree as ET

from src.gbif.gbif2bison import *
from src.gbif.constants import *
from src.gbif.gbifapi import GBIFCodes

subdir = SUBDIRS[0]
interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
outFname = os.path.join(DATAPATH, subdir, 'outBison.csv')
datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR)

gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)
gr.open()

uuid = 'ffb63b32-306e-415c-87a3-34c60d157a2a'
fname = os.path.join(gr._datasetPath, '{}.xml'.format(uuid))

vRecno = iRecno = 1
vline, vRecno = gr.getLine(gr._vCsvreader, vRecno)
iline, iRecno = gr.getLine(gr._iCsvreader, iRecno)
byline = gr.createBisonData(vline, iline)

currmeta = fnameElt.text



gr.extractData()

"""
   
   
   
         