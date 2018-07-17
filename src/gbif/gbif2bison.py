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
import os
import xml.etree.ElementTree as ET

from constants import *
from gbifapi import GBIFCodes
from tools import getCSVReader, getCSVWriter

# # .............................................................................
# def unicode_csv_reader(unicode_csv_data, dialect=unicodecsv.excel, delimiter=DELIMITER,
#                        **kwargs):
#    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
#    csv_reader = unicodecsv.reader(utf_8_encoder(unicode_csv_data),
#                            dialect=dialect, delimiter=delimiter, **kwargs)
#    for row in csv_reader:
#       # decode UTF-8 back to Unicode, cell by cell:
#       yield [unicode(cell, 'utf-8') for cell in row]
# 
# def utf_8_encoder(unicode_csv_data):
#    for line in unicode_csv_data:
#       yield line.encode('utf-8')

# .............................................................................
class GBIFReader(object):
   """
   @summary: GBIF Record containing CSV record of 
    * original provider data from verbatim.txt
    * GBIF-interpreted data from occurrence.txt
   """
   # ...............................................
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
      
   # ...............................................
   def open(self):
      '''
      @summary: Read metadata and open datafiles for reading
      '''
      self.fldMeta = self.getFieldMeta()
      
      (self._iCsvreader, 
       self._if) = getCSVReader(self.interpFname, DELIMITER)
       
      (self._vCsvreader, 
       self._vf)  = getCSVReader(self.verbatimFname, DELIMITER)
       
      (self._outWriter, 
       self._outf) = getCSVWriter(self.outFname, DELIMITER)

       
      # Pull the header row from each file, we use metadata for field indices
      _ = self.getLine(self._vCsvreader, 0)
      _ = self.getLine(self._iCsvreader, 0)
   
   # ...............................................
   def isOpen(self):
      """
      @note: returns True if either is open
      """
      if ((not self._if is None and not self._if.closed) or 
          (not self._vf is None and self._vf.closed)):
         return True
      return False

   # ...............................................
   def close(self):
      '''
      @summary: Close input datafiles
      '''
      self._if.close()
      self._vf.close()
      self._outf.close()

   # ...............................................
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

   # ...............................................
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

#    # .............................................................................
#    def getCSVReader(self, datafile, delimiter):
#       '''
#       @summary: Get a CSV reader that can handle encoding
#       '''
#       f = None  
#       unicodecsv.field_size_limit(sys.maxsize)
#       try:
#          f = open(datafile, 'rb')
#          csvreader = unicodecsv.reader(f, delimiter=delimiter, encoding=ENCODING)
#       except Exception, e:
#          raise Exception('Failed to read or open {}, ({})'
#                          .format(datafile, str(e)))
#       return csvreader, f
#    
#    # .............................................................................
# #    def getCSVDictWriter(self, datafile):
#    def getCSVWriter(self, datafile, delimiter):
#       '''
#       @summary: Get a CSV writer that can handle encoding
#       '''
#       unicodecsv.field_size_limit(sys.maxsize)
#       try:
#          f = open(datafile, 'wb') 
#          writer = unicodecsv.writer(f, delimiter=delimiter, encoding=ENCODING)
# 
#       except Exception, e:
#          raise Exception('Failed to read or open {}, ({})'
#                          .format(datafile, str(e)))
#       return writer, f

   # ...........................
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
   def createBisonLine(self, vline, iline):
      """
      @param verbLine: A CSV record of original provider DarwinCore occurrence data 
      @param intLine: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
      rec = {}
      gbifID = vline[0]
      lon = lat = cntry = None
      for fldname, meta in self.fldMeta.iteritems():
         # get column Index in correct file
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
            print('gbifID {}: Field {}, val {} prohibited'
                  .format(gbifID, fldname, val)) 
         # delete records with no canonical name
         if fldname == 'taxonKey':
            val = self._getCanonicalName(val)
            if val is None:
               print('gbifID {}: Field {}, val {} does not contain canonical'
                     .format(gbifID, fldname, val)) 
               return []
            else:
               rec['canonicalName'] = val
         # Compute? providerID
         elif fldname == 'publisher':
               rec['providerID'] = val
         # Compute? resourceID
         elif fldname == 'datasetKey':
               rec['resourceID'] = val
         # delete absence records
         elif fldname == 'occurrenceStatus':
            if val.lower() == 'absent':
               print('gbifID {}: Field {}, val {} is absence data'
                     .format(gbifID, fldname, val)) 
               return []
         # simplify basisOfRecord terms
         elif fldname == 'basisOfRecord':
            if val in TERM_CONVERT.keys():
               val = TERM_CONVERT[val]
               print('gbifID {}: Field {}, val {} converted'
                     .format(gbifID, fldname, val)) 
         # Convert year to integer
         elif fldname == 'year':
            try:
               val = int(val)
            except:
               print('gbifID {}: Field {}, val {} is not an integer'
                     .format(gbifID, fldname, val)) 
               val = None
         # check Long/Lat fields
         elif fldname == 'countryCode':
            cntry = val
         elif fldname == 'decimalLongitude':
            lon = float(val)
         elif fldname == 'decimalLatitude':
            lat = float(val)
         # Save modified val
         if fldname not in ('decimalLongitude', 'decimalLatitude', 'taxonKey',
                            'publisher', 'datasetKey'):
            rec[fldname] = val
         
      lon, lat = self._getPoint(lon, lat, cntry)
      rec['decimalLongitude'] = lon
      rec['decimalLatitude'] = lat
      # create the ordered row
      row = []
      for fld in ORDERED_OUT_FIELDS:
         try:
            row.append(rec[fld])
         except KeyError, e:
            row.append('')
      return row
   
   # ...............................................
   def extractData(self):      
      if self.isOpen():
         self.close()
      try:
         self.open()
         vRecno = iRecno = 1
         
         self._outWriter.writerow(ORDERED_OUT_FIELDS)
         while (self._vCsvreader is not None and 
                self._iCsvreader is not None and vRecno < 500):
            vline, vRecno = self.getLine(self._vCsvreader, vRecno)
            iline, iRecno = self.getLine(self._iCsvreader, iRecno)
            
            # Make sure same UUID/gbifID 
            if iline[0] == vline[0]:
               byline = self.createBisonData(vline, iline)
               self._outWriter.writerow(byline)
            else:
               print('Record {}/{} verbatim gbifID {} != interpreted gbifID {}'
                     .format(vRecno, iRecno, vline[0], iline[0]))
      finally:
         self.close()
         
# ...............................................
if __name__ == '__main__':
   subdir = SUBDIRS[0]
   interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
   verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
   outFname = os.path.join(DATAPATH, subdir, OUT_BISON)
   datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR) 
   gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)
   gr.extractData()
   
   
"""
import unicodecsv
import os
import sys
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

outWriter, outf = gr.getCSVWriter(gr.outFname, DELIMITER)
outWriter.writerow(ORDERED_OUT_FIELDS)


vRecno = iRecno = 1
for i in range(35):
   vline, vRecno = gr.getLine(gr._vCsvreader, vRecno)
   iline, iRecno = gr.getLine(gr._iCsvreader, iRecno)
   byline = gr.createBisonData(vline, iline)
   outWriter.writerow(byline)

vline, vRecno = gr.getLine(gr._vCsvreader, vRecno)
iline, iRecno = gr.getLine(gr._iCsvreader, iRecno)
byrow = gr.createBisonData(vline, iline)
brow = outWriter.writerow(byline)

currmeta = fnameElt.text


gr.extractData()

"""
   
   
   
         