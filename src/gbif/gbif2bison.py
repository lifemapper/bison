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
      @param verbatimFname: Full filename containing records from the GBIF 
             verbatim occurrence table
      @param interpretedFname: Full filename containing records from the GBIF 
             interpreted occurrence table
      @param metaFname: Full filename containing metadata for all data files in the 
             Darwin Core GBIF Occurrence download:
                https://www.gbif.org/occurrence/search
      @param outFname: Full filename for the output BISON CSV file
      @param datasetPath: Directory containing individual files describing 
             datasets referenced in occurrence records.
      """
      self.gbifRes = GBIFCodes()
      # Interpreted GBIF occurrence file
      self.interpFname = interpretedFname
      self._if = None
      self._iCsvreader = None
      # Verbatim GBIF occurrence file
      self.verbatimFname = verbatimFname
      self._vf = None
      self._vCsvreader = None
      # Output BISON occurrence file
      self.outFname = outFname
      self._outf = None
      self._outWriter = None
      pth = os.path.abspath(outFname)
      # Output Canonical name lookup file
      self.outCanonicalFname = os.path.join(pth, 'canonicalLookup.csv')
      self._outcf = None
      self._outCanWriter = None
      # Output Provider UUID lookup file
      self.outProviderFname = os.path.join(pth, 'providerLookup.csv')
      self._outpf = None
      self._outProvWriter = None
      # GBIF metadata file for occurrence files
      self._metaFname = metaFname
      # Path to GBIF provided dataset metadata files
      self._datasetPath = datasetPath

      self._datasetPubs = {}

      self.fldMeta = None
      self.dsMeta = {}
      self._files = [self._if, self._vf, self._outf, self._outcf, self._outpf]
      
   # ...............................................
   def _cleanVal(self, val):
      val = val.strip()
      # TODO: additional conversion of unicode?
      if val.lower() in PROHIBITED_VALS:
         val = ''
      return val

   # ...............................................
   def _getValFromCorrectLine(self, fldname, meta, vline, iline):
      """
      @summary: IFF gathering values from separate lines (matched on gbifID)
                use metadata to pull the value from the indicated line.
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
      """
      @summary: Update the decimal longitude and latitude, replacing 0,0 with 
                None, None and ensuring that US points have a negative longitude.
      @param rec: dictionary of all fieldnames and values for this record
      """
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
      @summary: Fill missing publisher/providerID by querying the GBIF API 
               for this datasetKey.  Save retrieved values in a dictionary to 
               avoid re-querying the same datasetKeys.
      @param rec: dictionary of all fieldnames and values for this record
      """
      datasetKey = rec['resourceID']
      pubID = rec['providerID']
      if pubID is None or pubID == '':
         try: 
            pubID = self._datasetPubs[datasetKey]
         except:
            if datasetKey is not None:
               pubID = self.gbifRes.getProviderFromDatasetKey(datasetKey)
               self._datasetPubs[datasetKey] = pubID
               rec['providerID'] = pubID
      provValues = [datasetKey, pubID]
      self._outProvWriter.writerow(provValues)

   # ...............................................
   def _updateYear(self, rec):
      """
      @todo: This function does not yet fill values
      @summary: Update missing year values by extracting it from the eventDate.
      @param rec: dictionary of all fieldnames and values for this record
      """
      if rec['year'] is None:
         dt = rec['eventDate']
         if dt is not None and dt != '':
            print('Event date is {}!'.format(dt))
            

   # ...............................................
   def _getCanonical(self, rec):
      """
      @summary: Fill canonicalName by querying the GBIF API. First use the
                scientificName in the GBIF name parser.  If that fails, 
                query the GBIF species API with the taxonKey.
      @param rec: dictionary of all fieldnames and values for this record
      @note: The name parser fails on unicode namestrings
      @todo: Save file(s?) with 'gbifID, scientificName, taxonKey, canonicalName' 
             to minimize re-queries on the same species or re-run.
      """
      canName = sciname = taxkey = None
      try:
         sciname = rec['scientificName']
         canName = self.gbifRes.resolveCanonicalFromScientific(sciname)
         print('gbifID {}: Canonical {} from scientificName'
               .format(rec['gbifID'], canName)) 
      except:
         try:
            taxkey = rec['taxonKey']
            canName = self.gbifRes.resolveCanonicalFromTaxonKey(taxkey)
            print('gbifID {}: Canonical {} from taxonKey'
                  .format(rec['gbifID'], canName)) 
         except:
            print('gbifID {}: Failed to get canonical'.format(rec['gbifID']))
      rec['canonicalName'] = canName
      canonicalValues = [sciname, taxkey, canName]
      self._outCanWriter.writerow(canonicalValues)

   # ...............................................
   def _updateFieldOrSignalRemove(self, gbifID, fldname, val):
      """
      @summary: Update fields with any BISON-requested changed, or signal 
                to remove the record by returning None for fldname and val.
      @param gbifID: GBIF identifier for this record, used just for logging
      @param fldname: Fieldname in current record
      @param val: Value for this field in current record
      """
      # Replace N/A
      if val.lower() in PROHIBITED_VALS:
         val = ''
         print('gbifID {}: Field {}, val {} prohibited'
               .format(gbifID, fldname, val)) 
         
      # Get providerID
      elif fldname == 'publisher':
         fldname = 'providerID'

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
      @summary: Read and populate metadata, open datafiles for reading, output
                files for writing
      @todo: Save canonicalNames and provider UUIDs into a file to avoid 
             querying repeatedly.
      '''
      self.openInterpreted()
       
      (self._vCsvreader, 
       self._vf)  = getCSVReader(self.verbatimFname, DELIMITER)
       
      # Pull the header row from each file, we use metadata for field indices
      _ = self.getLine(self._vCsvreader, 0)
   
   # ...............................................
   def openInterpreted(self):
      '''
      @summary: Read and populate metadata, open datafiles for reading, output
                files for writing
      @todo: Save canonicalNames and provider UUIDs into a file to avoid 
             querying repeatedly.
      '''
      self.fldMeta = self.getFieldMetaFromInterpreted()
      
      (self._iCsvreader, 
       self._if) = getCSVReader(self.interpFname, DELIMITER)
       
      (self._outWriter, 
       self._outf) = getCSVWriter(self.outFname, DELIMITER)
       
      (self._outCanWriter, 
       self._outcf) = getCSVWriter(self.outCanonicalFname, DELIMITER)
       
       
      (self._outProvWriter, 
       self._outpf) = getCSVWriter(self.outProviderFname, DELIMITER)
       
      # Pull the header row 
      _ = self.getLine(self._iCsvreader, 0)

   # ...............................................
   def isOpen(self):
      """
      @summary: Return true if any files are open.
      """
      for f in self._files:
         if not f is None and not f.closed:
            return True
      return False

   # ...............................................
   def close(self):
      '''
      @summary: Close input datafiles and output file
      '''
      for f in self._files:
         try:
            f.close()
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
#    def getDatasetTitle(self, uuid):
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
   def _updateFilterRec(self, rec):
      """
      @summary: Update record with all BISON-requested changes, or remove 
                the record by setting it to None.
      @param rec: dictionary of all fieldnames and values for this record
      """
      gbifID = rec['gbifID']
      # If publisher is missing from record, use API to get from datasetKey
      # Ignore BISON records 
      self._fillPublisher(rec)
      if rec['providerID'] == BISON_UUID:
         print ('gbifID {} from BISON publisher'.format(gbifID))
         rec = None

      # Ignore absence record 
      if rec and rec['occurrenceStatus'].lower() == 'absent':
         print('gbifID {} is absence data'.format(gbifID)) 
         rec = None
      
      if rec:
         self._updateYear(rec)
         
         # Ignore record without canonicalName
         self._getCanonical(rec)
         if rec['canonicalName'] is None:
            rec = None

      if rec:
         # Modify lat/lon vals if necessary
         self._updatePoint(rec)

   # ...............................................
   def createBisonLineFromInterpreted(self, iline):
      """
      @summary: Create a list of values, ordered by BISON-requested fields in 
                ORDERED_OUT_FIELDS, with individual values and/or entire record
                modified according to BISON needs.
      @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
      @return: list of ordered fields containing BISON-interpreted values for 
               a single GBIF occurrence record. 
      """
      row = []
      gbifID = iline[0]
      rec = {'gbifID': gbifID}
      for fldname, (idx, dtype) in self.fldMeta.iteritems():
         val = self._cleanVal(iline[idx])

         fldname, val = self._updateFieldOrSignalRemove(gbifID, fldname, val)
         if fldname is None:
            return row
         else:
            rec[fldname] = val
            
      # Update values and/or filter record out
      self._updateFilterRec(rec)
      
      if rec:
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
      @summary: Create a list of values, ordered by BISON-requested fields in 
                ORDERED_OUT_FIELDS, with individual values and/or entire record
                modified according to BISON needs.
      @param vline: A CSV record of original provider DarwinCore occurrence data 
      @param iline: A CSV record of GBIF-interpreted DarwinCore occurrence data
      @return: list of ordered fields containing BISON-interpreted values for 
               a single GBIF occurrence record. 
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

      # Update values and/or filter record out
      self._updateFilterRec(rec)
      
      if rec:
         # create the ordered row
         for fld in ORDERED_OUT_FIELDS:
            try:
               row.append(rec[fld])
            except KeyError, e:
               row.append('')

      return row
         
   # ...............................................
   def extractData(self):
      """
      @summary: Create a CSV file containing GBIF occurrence records extracted 
                from the verbatim and interpreted occurrence files provided 
                from an Occurrence Download, in Darwin Core format.  Records 
                are assembled by matching gbifIDs from each file, and pulling 
                some values from each file. Values may be modified and records 
                may be discarded according to BISON requests.
      @return: A CSV file of BISON-modified records from a GBIF download. 
      """
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
      """
      @summary: Create a CSV file containing GBIF occurrence records extracted 
                from the interpreted occurrence file provided 
                from an Occurrence Download, in Darwin Core format.  Values may
                be modified and records may be discarded according to 
                BISON requests.
      @return: A CSV file of BISON-modified records from a GBIF download. 
      """
      if self.isOpen():
         self.close()
      try:
         self.openInterpreted()
         iRecno = 1
         
         self._outWriter.writerow(ORDERED_OUT_FIELDS)
         while (self._iCsvreader is not None and iRecno <= 500):
            # Get interpreted record
            iline, iRecno = self.getLine(self._iCsvreader, iRecno)
            if iline is None:
               break
            coreId = iline[0]
            # Create new record
            byline = self.createBisonLineFromInterpreted(iline)
            if byline:
               self._outWriter.writerow(byline)
      finally:
         self.close()

# ...............................................
if __name__ == '__main__':
   subdir = SUBDIRS[0]
#    interpFname = os.path.join(DATAPATH, subdir, INTERPRETED)
   interpFname = os.path.join(DATAPATH, subdir, 'interpretedTerritories500.csv')
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
      
gr._updateFilterRec(rec)

gr.close()

# byline = self.createBisonLineFromInterpreted(iline)

# gr.extractData()

"""
   
   
   
         