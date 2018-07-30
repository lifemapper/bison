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
      self._datasetPubs = {}
      self._datasetPath = datasetPath
      self._if = None
      self._vf = None
      self._iCsvreader = None
      self._vCsvreader = None
      self._metaFname = metaFname
      self.fldMeta = None
      self.dsMeta = {}
      
   # ...............................................
   def _cleanVal(self, val):
      val = val.strip()
      if val.lower() in PROHIBITED_VALS:
         val = ''
      return val

   # ...............................................
   def _getValFromCorrectLine(self, fldname, meta, vline, iline):
      """
      @param fldname: field name 
      @param meta: tuple including datatype and INTERPRETED or VERBATIM 
                   identifier for file to pull value from
      @param vline: A CSV record of original provider DarwinCore occurrence data 
      @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
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
            try:
               val = vline[self.fldMeta[fldname][VERBATIM]]
            except Exception, e:
               print'{} not in either file'.format(fldname)
      return val

   # ...............................................
   def _updatePoint(self, rec):
      try:
         rec['decimalLongitude']
      except:
         rec['decimalLongitude'] = None

      try:
         rec['decimalLatitude']
      except:
         rec['decimalLatitude'] = None

      try:
         cntry = rec['countryCode']
      except:
         rec['countryCode'] = None
         
      # Change 0,0 to None
      if rec['decimalLongitude'] == 0 and rec['decimalLatitude'] == 0:
         rec['decimalLongitude'] = None
         rec['decimalLatitude'] = None
      # Make sure US longitude is negative
      elif (cntry == 'US' 
            and rec['decimalLongitude'] is not None 
            and rec['decimalLongitude'] > 0):
         rec['decimalLongitude'] = rec['decimalLongitude'] * -1 

   # ...............................................
   def _fillPublisher(self, rec):
      """
      @summary: If publisher is missing from record, use API to get from 
               datasetKey.  Save values in a dictionary to avoid re-querying
               same values.
      """
      datasetKey = rec['resourceID']
      pubID = rec['providerID']
      if pubID is None or pubID == '':
         try: 
            pubId = self._datasetPubs[datasetKey]
         except:
            if datasetKey is not None:
               pubID = self.codeResolver.getProviderFromDatasetKey(datasetKey)
               self._datasetPubs[datasetKey] = pubID
         rec['providerID'] = pubID

   # ...............................................
   def _getCanonical(self, rec):
      # Try from scientificName first, then taxonKey
      canName = None
      try:
         canName = self.codeResolver.resolveCanonicalFromScientific(rec['scientificName'])
      except:
         print('gbifID {}: Could not get canonical'
               .format(rec['gbifID'])) 
         try:
            canName = self.codeResolver.resolveCanonicalFromTaxonKey(rec['taxonKey'])
         except:
            print('gbifID {}: Could not get canonical from taxonKey {}'
                  .format(rec['gbifID'], rec['taxonKey'])) 
      return canName

   # ...............................................
   def _updateFieldOrSignalRemove(self, gbifID, fldname, val):
      # Replace N/A
      if val.lower() in PROHIBITED_VALS:
         val = ''
         print('gbifID {}: Field {}, val {} prohibited'
               .format(gbifID, fldname, val)) 
         
      # Get providerID, remove if BISON UUID
      elif fldname == 'publisher':
         fldname = 'providerID'
         if val == BISON_UUID:
            print ('Found BISON publisher')
            fldname = val = None

      # Get resourceID
      elif fldname == 'datasetKey':
         fldname = 'resourceID'
               
      # simplify basisOfRecord terms
      elif fldname == 'basisOfRecord':
         if val in TERM_CONVERT.keys():
            val = TERM_CONVERT[val]
            print('gbifID {}: basisOfRecord converted to {}'.format(gbifID, val)) 
            
      # Convert year to integer
      elif fldname == 'year':
         try:
            val = int(val)
         except:
            if val != '':
               print('gbifID {}: Year {} is not an integer'.format(gbifID, val))
            val = None 

      # remove records with occurrenceStatus  = absence
      elif fldname == 'occurrenceStatus' and val.lower() == 'absent':
         fldname = val = None
         print('gbifID {}: record is absence data'.format(gbifID)) 
         
      # gather geo fields for check/convert
      elif fldname in ('decimalLongitude', 'decimalLatitude'):
         try:
            val = float(val)
         except Exception, e:
            if val != '':
               print('gbifID {}: {} lat/long is not number'.format(gbifID, val))
            val = None
         
      return fldname, val

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
   def openInterpreted(self):
      '''
      @summary: Read metadata and open datafiles for reading
      '''
      self.fldMeta = self.getFieldMetaFromInterpreted()
      
      (self._iCsvreader, 
       self._if) = getCSVReader(self.interpFname, DELIMITER)
       
      (self._outWriter, 
       self._outf) = getCSVWriter(self.outFname, DELIMITER)

       
      # Pull the header row 
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
      try:
         self._if.close()
      except Exception, e:
         pass
      try:
         self._vf.close()
      except Exception, e:
         pass
      try:
         self._outf.close()
      except Exception, e:
         pass

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
         sortedname = 'tidy_' + currmeta.replace('txt', 'csv')
         if sortedname in (INTERPRETED, VERBATIM):
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
   def getFieldMetaFromInterpreted(self):
      '''
      @summary: Read metadata for interpreted data file, and
                for fields we are interested in:
                extract column index for each file, add datatype. 
                Resulting metadata will look like:
                     fields = {term: (columnIndex, dtype), 
                               ...
                               }
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
         sortedname = 'tidy_' + currmeta.replace('txt', 'csv')
         if sortedname == INTERPRETED:
            flds = child.findall(tdwg+'field')
            for fld in flds:
               idx = int(fld.get('index'))
               temp = fld.get('term')
               term = temp[temp.rfind(CLIP_CHAR)+1:]
               if term in SAVE_FIELDS.keys():
                  dtype = SAVE_FIELDS[term][0]
               else:
                  dtype = str
               if not fields.has_key(term):
                  fields[term] = (idx, dtype)
      return fields

#    # ...............................................
#    def getDatasetMeta(self, uuid):
#       '''
#       @summary: Read metadata for dataset with this uuid
#       '''
#       fname = os.path.join(self._datasetPath, '{}.txt'.format(uuid))
#       tree = ET.parse(fname)
#       root = tree.getroot()
#       ds  = root.find('dataset')
#       title = ds.find('title').text
#       return title
      
   # ...............................................
   def createBisonLineFromInterpreted(self, iline):
      """
      @param vline: A CSV record of original provider DarwinCore occurrence data 
      @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
      rec = {}
      row = []
      gbifID = iline[0]
      for fldname, (idx, dtype) in self.fldMeta.iteritems():
         val = iline[idx]

         fldname, val = self._updateFieldOrSignalRemove(gbifID, fldname, val)
         if fldname is None:
            return row
         else:
            rec[fldname] = val
            
      # If publisher is missing from record, use API to get from datasetKey
      self._fillPublisher(rec)

      # Modify lat/lon vals if necessary
      self._updatePoint(rec)
      
      # Ignore record without canonicalName
      canName = self._getCanonical(rec)
      if canName is None:
         return row
         
      # Ignore absence record 
      if rec['occurrenceStatus'].lower() == 'absent':
         print('gbifID {}: Field {}, val {} is absence data'
               .format(gbifID, fldname, val)) 
         return row
      
      # create the ordered row
      for fld in ORDERED_OUT_FIELDS:
         try:
            row.append(rec[fld])
         except KeyError, e:
            row.append('')

      return row
   
   # ...............................................
   def createBisonLine(self, vline, iline):
      """
      @param vline: A CSV record of original provider DarwinCore occurrence data 
      @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data 
      """
      row = []
      rec = {}
      gbifID = iline[0]
      for fldname, meta in self.fldMeta.iteritems():
         val = self._getValFromCorrectLine(fldname, meta, vline, iline)
         fldname, val = self._updateFieldOrSignalRemove(gbifID, fldname, val)
         if fldname is None:
            return row
         else:
            rec[fldname] = val

      # If publisher is missing from record, use API to get from datasetKey
      self._fillPublisher(rec)

      # Modify lat/lon vals if necessary
      self._updatePoint(rec)
      
      # Ignore record without canonicalName
      canName = self._getCanonical(rec)
      if canName is None:
         return row
         
      # Ignore absence record 
      if rec['occurrenceStatus'].lower() == 'absent':
         print('gbifID {}: Field {}, val {} is absence data'
               .format(gbifID, fldname, val)) 
         return row
      
      # create the ordered row
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
   def extractDataFromInterpreted(self):      
      if self.isOpen():
         self.close()
      try:
         self.openInterpreted()
         iRecno = 1
         
         self._outWriter.writerow(ORDERED_OUT_FIELDS)
         while (self._iCsvreader is not None and iRecno < 500):
            # Get interpreted record
            iline, iRecno = self.getLine(self._iCsvreader, iRecno)
            if iline is None:
               break
            coreId = iline[0]
            # Create new record
            byline = self.createBisonLineFromInterpreted(iline)
            self._outWriter.writerow(byline)
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
   gr.extractDataFromInterpreted()
#    gr.extractData()
   
   
"""
import unicodecsv
import os
import sys
import xml.etree.ElementTree as ET

from src.gbif.gbif2bison import *
from src.gbif.constants import *
from src.gbif.gbifapi import GBIFCodes
from src.gbif.gbif2bison import GBIFReader

subdir = SUBDIRS[0]
interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM)
outFname = os.path.join(DATAPATH, subdir, OUT_BISON)
datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR)

gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)


gr.openInterpreted()
fm = gr.fldMeta
iRecno = 1

iline, iRecno = gr.getLine(gr._iCsvreader, iRecno)
rec = {}
gbifID = iline[0]
for fldname, (idx, dtype) in gr.fldMeta.iteritems():
   print
   print fldname
   val = iline[idx]
   fldname, val = gr._updateFieldOrSignalRemove(gbifID, fldname, val)
   if fldname is None:
      print('Removed with bad field')
   else:
      rec[fldname] = val
      print fldname, val
      
gr._fillPublisher(rec)
gr._updatePoint(rec)
canName = self._getCanonical(rec)
if canName is None:
   print('gbifID {}: Field {}, val {} is absence data'
         .format(gbifID, fldname, val)) 

 
if rec['occurrenceStatus'].lower() == 'absent':
   print('gbifID {}: Field {}, val {} is absence data'
         .format(gbifID, fldname, val)) 



# byline = self.createBisonLineFromInterpreted(iline)

# gr.extractData()

"""
   
   
   
         