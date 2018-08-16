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
import glob
import os
import xml.etree.ElementTree as ET

from constants import *
from tools import getCSVReader, getCSVWriter, getLogger

# .............................................................................
class GBIFReaderOld(object):
   """
   @summary: GBIF Record containing CSV record of 
             * original provider data from verbatim.txt
             * GBIF-interpreted data from occurrence.txt
   @note: To chunk the file into more easily managed small files (i.e. fewer 
          GBIF API queries), split using sed command output to file like: 
            sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
          where 1-500 are lines to delete, and 1500 is the line on which to stop.
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
#       self.gbifRes = GBIFCodes()
      self._files = []
      
      # Interpreted GBIF occurrence file
      self.interpFname = interpretedFname
      self._if = None
      self._files.append(self._if)
      self._iCsvrdr = None
      # GBIF metadata file for occurrence files
      self._metaFname = metaFname
      self.fldMeta = None
      # Path to GBIF provided dataset metadata files
      self._datasetPath = datasetPath
      
      # Verbatim GBIF occurrence file
      self.verbatimFname = verbatimFname
      self._vf = None
      self._files.append(self._vf)
      self._vCsvrdr = None

      # Output BISON occurrence file
      if os.path.exists(outFname):
         os.remove(outFname)
      self.outFname = outFname
      self._outf = None
      self._files.append(self._outf)
      self._outWriter = None
      pth, outbasename = os.path.split(outFname)
      outbase, ext = os.path.splitext(outbasename)
      
      # Output Canonical name lookup file
      self.outCanonicalFname = os.path.join(pth, 'canonicalLookup.csv')
      self._outcf = None
      self._files.append(self._outcf)
      self._outCanWriter = None
      # Output Organization UUID lookup file
      self.outOrgFname = os.path.join(pth, 'orgLookup.csv')
      self._outof = None
      self._files.append(self._outof)
      self._outOrgWriter = None
      # Output Dataset UUID lookup file
      self.outDSFname = os.path.join(pth, 'datasetLookup.csv')
      self._outdf = None
      self._files.append(self._outdf)
      self._outDSWriter = None

      self._datasetLookup = {}
      self._orgLookup = {}
      self._nameLookup = {}

      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(pth, outbase + '.log')
      if os.path.exists(logfname):
         os.remove(logfname)
      self._log = getLogger(logname, logfname)
      
# .............................................................................
class GBIFReader(object):
   """
   @summary: GBIF Record containing CSV record of 
             * original provider data from verbatim.txt
             * GBIF-interpreted data from occurrence.txt
   @note: To chunk the file into more easily managed small files (i.e. fewer 
          GBIF API queries), split using sed command output to file like: 
            sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
          where 1-500 are lines to delete, and 1500 is the line on which to stop.
   """
   # ...............................................
   def __init__(self, interpretedFname, metaFname, outFname):
      """
      @summary: Constructor
      @param interpretedFname: Full filename containing records from the GBIF 
             interpreted occurrence table
      @param metaFname: Full filename containing metadata for all data files in the 
             Darwin Core GBIF Occurrence download:
                https://www.gbif.org/occurrence/search
      @param outFname: Full filename for the output BISON CSV file
      """
#       self.gbifRes = GBIFCodes()
      self._files = []
      
      # Interpreted GBIF occurrence file
      self.interpFname = interpretedFname
      self._if = None
      self._files.append(self._if)
      self._iCsvrdr = None
      # GBIF metadata file for occurrence files
      self._metaFname = metaFname
      self.fldMeta = None
      
      # Output BISON occurrence file
      if os.path.exists(outFname):
         os.remove(outFname)
      self.outFname = outFname
      self._outf = None
      self._files.append(self._outf)
      self._outWriter = None
      pth, outbasename = os.path.split(outFname)
      outbase, ext = os.path.splitext(outbasename)
      
      # Input for Canonical name lookup
      self.name4LookupFname = os.path.join(pth, 'nameUUIDForLookup.csv')
      self._name4lookupf = None
      self._files.append(self._name4lookupf)
      self._name4lookupWriter = None
      self._name4lookup = {}

      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(pth, outbase + '.log')
      if os.path.exists(logfname):
         os.remove(logfname)
      self._log = getLogger(logname, logfname)
      
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

#    # ...............................................
#    def _fillOrganizationDatasetVals(self, rec):
#       """
#       @summary: Fill missing organization and dataset values by querying the 
#                GBIF API using the GBIF UUIDs.  Save retrieved values in 2 
#                dictionaries to avoid re-querying the same UUIDs.
#       @param rec: dictionary of all fieldnames and values for this record
#       @note: For both Organizations (provider/publisher) and Datasets (resource)
#              we want to get info from the GBIF API including 
#              GBIF UUID, name/code, and URL.  
#              Save to 2 files to minimize re-queries for matching 
#              datasets/organizations and testing. Lines look like:
#             'orgUUID, orgName, orgUrl'
#             'datasetUUID, datasetName, datasetHomepage, orgUUID'
#       @note: orgUUIDOrganization UUID may be pulled from dataset API if missing.
#       @note: Lookup dictionaries are:
#                    {orgUUID: (orgName, orgHomepage),
#                     ...}  
#                    {datasetUUID: (dsName, dsHomepage, orgUUID),
#                     ...}        
#       """
#       # Dataset/resource/collection
#       datasetUUID = rec['resourceID']
#       dsCode = dsUrl = orgUUID = ''      
#       if datasetUUID != '':
#          try: 
#             (dsCode, dsUrl, orgUUID) = self._datasetLookup[datasetUUID]
#          except:
#             dsDict = self.gbifRes.resolveDataset(datasetUUID)
#             try:
#                orgUUID = dsDict['publishingOrganizationKey']
#                dsCode = dsDict['title']
#                dsUrl = dsDict['homepage']
#                if type(dsUrl) is list and len(dsUrl) > 0:
#                   dsUrl = dsUrl[0]
#                # Save dataset values to lookup
#                self._datasetLookup[datasetUUID] = (dsCode, dsUrl, orgUUID)
#                # Save dataset values to file
#                self._outDSWriter.writerow([datasetUUID, dsCode, dsUrl, orgUUID])
#             except:
#                pass
#             
#       # Save dataset values to record, provided values take precedence
#       if rec['ownerInstitutionCode'] == '':
#          rec['ownerInstitutionCode'] = dsCode
# 
#       if rec['collectionID'] == '':
#          rec['collectionID'] = dsUrl
# 
#       rec['providerID'] = orgUUID
#                
#       # Organization/provider/institution
#       orgCode = orgUrl = ''         
#       try: 
#          (orgCode, orgUrl) = self._orgLookup[orgUUID]
#       except:
#          orgDict = self.gbifRes.resolveOrganization(orgUUID)
#          try:
#             orgCode = orgDict['title']
#             orgUrl = orgDict['homepage']
#             if type(orgUrl) is list and len(orgUrl) > 0:
#                orgUrl = orgUrl[0]
#             # Save org values to lookup
#             self._orgLookup[orgUUID] = (orgCode, orgUrl)
#             # Save org values to file
#             self._outOrgWriter.writerow([orgUUID, orgCode, orgUrl])
#          except:
#             pass
#          
#       # Save org values to record, provided values take precedence
#       if rec['institutionCode'] == '':
#          rec['institutionCode'] = orgCode
# 
#       if rec['institutionID'] == '':
#          rec['institutionID'] = orgUrl

   # ...............................................
   def _correctDates(self, rec):
      """
      @summary: Make sure that eventDate is parse-able into integers and update 
                missing year value by extracting from the eventDate.
      @param rec: dictionary of all fieldnames and values for this record
      @note: BISON eventDate should be ISO 8601, ex: 2018-08-01
             GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
      """
      fillyr = False
      # Test year field
      try:
         rec['year'] = int(rec['year'])
      except:
         fillyr = True
         
      # Test eventDate field
      tmp = rec['eventDate']
      dateonly = tmp.split('T')[0]
      if dateonly != '':
         parts = dateonly.split('-')
         try:
            for i in range(len(parts)):
               int(parts[i])
         except:
            pass
         else:
            rec['eventDate'] = dateonly
            if fillyr:
               rec['year'] = parts[0]

   # ...............................................
   def _saveNameLookupData(self, rec):
      """
      @summary: Save scientificName and taxonID for parse or API query 
                respectively. 
      @param rec: dictionary of all fieldnames and values for this record
      @note: The name parser fails on unicode namestrings
      @note: Save to dictionary key=sciname, val=taxonid to avoid writing 
             duplicates.  Append to file to save for later script runs, with 
             lines like:
                 'scientificName, taxonKey [, taxonKey, taxonKey ...]'
      """
      sciname = taxkey = None
      try:
         sciname = rec['scientificName']
      except:
         sciname = ''
         
      try:
         taxkey = rec['taxonKey']
      except:
         taxkey = ''
         
      try:
         keylist = self._name4lookup[sciname]
      except:
         self._name4lookup[sciname] = [taxkey]
      else:
         if taxkey in keylist:
            pass
         else:
            self._name4lookup[sciname].append(taxkey)
            self._log.warn('Sciname {} has multiple taxonIDs {}'.format(
               sciname, self._name4lookup[sciname]))

#    # ...............................................
#    def _fillCanonical(self, rec):
#       """
#       @summary: Fill canonicalName by querying the GBIF API. First use the
#                 scientificName in the GBIF name parser.  If that fails, 
#                 query the GBIF species API with the taxonKey.
#       @param rec: dictionary of all fieldnames and values for this record
#       @note: The name parser fails on unicode namestrings
#       @note: Save to file(s?) with lines like:
#                  'scientificName, taxonKey, canonicalName'
#              to minimize re-queries on the same species or re-run.
#       @note: Lookup dictionary is {sciname: canonicalName,
#                                    taxonKey: canonicalName,
#                                    ...}  
#       """
#       canName = sciname = taxkey = None
#       try:
#          sciname = rec['scientificName']
#          canName = self._nameLookup[sciname]
#       except:
#          try:
#             canName = self.gbifRes.resolveCanonicalFromScientific(sciname)
#             self._nameLookup[sciname] = canName
#             canonicalValues = [sciname, taxkey, canName]
#             self._outCanWriter.writerow(canonicalValues)
#  
#          except:
#             try:
#                taxkey = rec['taxonKey']
#                canName = self._nameLookup[taxkey]
#             except:
#                try:
#                   canName = self.gbifRes.resolveCanonicalFromTaxonKey(taxkey)
#                   self._nameLookup[taxkey] = canName
#                   canonicalValues = [sciname, taxkey, canName]
#                   self._outCanWriter.writerow(canonicalValues)
#                except:
#                   pass
#       rec['canonicalName'] = canName

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
         
#       # Get providerID (aka publisher, organization)
#       elif fldname == 'publisher':
#          fldname = 'providerID'
# 
#       # Get resourceID (aka dataset, collection)
#       elif fldname == 'datasetKey':
#          fldname = 'resourceID'
               
      # simplify basisOfRecord terms
      elif fldname == 'basisOfRecord':
         if val in TERM_CONVERT.keys():
            val = TERM_CONVERT[val]
            
      # Convert year to integer
      elif fldname == 'year':
         try:
            val = int(val)
         except:
            val = '' 

      # remove records with occurrenceStatus  = absence
      elif fldname == 'occurrenceStatus' and val.lower() == 'absent':
         fldname = val = None
         
      # gather geo fields for check/convert
      elif fldname in ('decimalLongitude', 'decimalLatitude'):
         try:
            val = float(val)
         except Exception, e:
            val = None
         
      return fldname, val

   # ...............................................
   def _openForReadWrite(self, fname, numKeys=1, numVals=1, header=None):
      '''
      @summary: Read and populate lookup table if file exists, open and return
                file and csvwriter for writing or appending. If lookup file 
                is new, write header if provided.
      '''
      lookupDict = {}
      doAppend = False
      
      if os.path.exists(fname):
         doAppend = True
         colCount = 2
         if header is not None:
            colCount = len(header)
         if (numKeys + numVals) != colCount:
            raise Exception('Column count != keys + vals')
         keyIdxs = []
         valIdxs = []      
         for i in range(colCount):
            if i < numKeys:
               keyIdxs.append(i)
            else:
               valIdxs.append(i)
         
         try:
            csvRdr, infile = getCSVReader(fname, DELIMITER)
            # get header
            line, recno = self.getLine(csvRdr, 0)
            # read lookup vals into dictionary
            while (line is not None):
               line, recno = self.getLine(csvRdr, recno)
               if line and len(line) == len(valIdxs):
                  try:
                     # read one or more values
                     if len(valIdxs) == 1:
                        val = line[valIdxs[0]]
                     else:
                        val = [line[v] for v in valIdxs]
                     # read one or more keys
                     for k in range(numKeys):
                        lookupDict[line[k]] = val
                  except Exception, e:
                     self._log.warn('Failed to read line {} from {}'
                                    .format(recno, fname))
            self._log.info('Read lookup file {}'.format(fname))
         finally:
            infile.close()
      
      outWriter, outfile = getCSVWriter(fname, DELIMITER, doAppend=doAppend)
      self._log.info('Re-opened lookup file {} for appending'.format(fname))

      if not doAppend and header is not None:
         outWriter.writerow(header)
      
      return lookupDict, outWriter, outfile 
   
   
   # ...............................................
   def _openLookupInputForWrite(self, fname, header=None):
      '''
      @summary: Write output opulate lookup table if file exists, open and return
                file and csvwriter for writing or appending. If lookup file 
                is new, write header if provided.
      '''
      doAppend = False
      if os.path.exists(self.outCanonicalFname):
         doAppend = True
      
      outWriter, outfile = getCSVWriter(self.outCanonicalFname, 
                                        DELIMITER, doAppend=True)
      self._log.info('Opened Scientific Name/TaxonID file {} for appending'.format(fname))

      if not doAppend:
         outWriter.writerow(['scientificName', 'taxonKey'])
      
      return outWriter, outfile 
   
   
   # ...............................................
   def openInputOutput(self):
      '''
      @summary: Read GBIF metadata, open GBIF interpreted data for reading, 
                output file for writing
      '''
      self.fldMeta = self.getFieldMeta()
      
      (self._iCsvrdr, 
       self._if) = getCSVReader(self.interpFname, DELIMITER)
       
      (self._outWriter, 
       self._outf) = getCSVWriter(self.outFname, DELIMITER, doAppend=False)
      # Write the header row 
      self._outWriter.writerow(ORDERED_OUT_FIELDS)
      self._log.info('Opened input/output files')
       

   # ...............................................
   def openReadLookups(self):
      '''
      @summary: Read lookup files, then re-open for appending to 
                save canonicalNames and organization UUIDs into files to avoid 
                querying repeatedly.
      '''
      self._nameLookup, self._outCanWriter, self._outcf = \
            self._openForReadWrite(self.outCanonicalFname, numKeys=2, numVals=1,
                        header=['scientificName', 'taxonKey', 'canonicalName'])
            
      self._orgLookup, self._outOrgWriter, self._outof = \
            self._openForReadWrite(self.outOrgFname, numKeys=1, numVals=2,
                        header=['orgUUID', 'title', 'homepage'])
       
      self._datasetLookup, self._outDSWriter, self._outdf = \
            self._openForReadWrite(self.outDSFname, numKeys=1, numVals=3,
                        header=['datasetUUID', 'title', 'homepage', 'orgUUID'])


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
         try:
            line = csvreader.next()
            if line:
               recno += 1
            success = True
         except OverflowError, e:
            recno += 1
            self._log.info( 'Overflow on record {}, line {} ({})'
                            .format(recno, csvreader.line, str(e)))
         except StopIteration:
            self._log.info('EOF after record {}, line {}'
                           .format(recno, csvreader.line_num))
            success = True
         except Exception, e:
            recno += 1
            self._log.info('Bad record on record {}, line {} ({})'
                           .format(recno, csvreader.line, e))

      return line, recno
   
   # ...............................................
   def getFieldMeta(self):
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
         if sortedname.find(INTERPRETED) >= 0:
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
#       self._fillOrganizationDatasetVals(rec)
#       if rec['providerID'] == BISON_UUID:
#          self._log.info('gbifID {} from BISON publisher discarded'.format(gbifID))
#          rec = None

      # Ignore absence record 
      if rec and rec['occurrenceStatus'].lower() == 'absent':
         self._log.info('gbifID {} with absence data discarded'.format(gbifID)) 
         rec = None
      
      # Format eventDate and fill missing year
      if rec:
         self._correctDates(rec)
         
         self._saveNameLookupData(rec)
         
#          # Ignore record without canonicalName
#          self._fillCanonical(rec)
#          if rec['canonicalName'] is None or rec['canonicalName'] == '':
#             self._log.info('gbifID {} with unresolvable name/key discarded'.format(gbifID)) 
#             rec = None

      if rec:
         # Modify lat/lon vals if necessary
         self._updatePoint(rec)

   # ...............................................
   def createBisonLine(self, iline):
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
      #self._log.info('')
      #self._log.info('Record {}'.format(gbifID))
      for fldname, (idx, dtype) in self.fldMeta.iteritems():
         try:
            tmpval = iline[idx]
         except Exception, e:
            self._log.warning('Failed to get column {}/{} for gbifID {}'
                              .format(idx, fldname, gbifID))
            return row
         else:
            val = self._cleanVal(tmpval)
   
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
               print ('Missing field {} in record with gbifID {}'
                      .format(fld, gbifID))
               row.append('')

      return row

   # ...............................................
   def extractData(self):
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
         self._name4lookup, self._name4lookupWriter, self._name4lookupf = \
               self._openForReadWrite(self.name4LookupFname,
                                      header=['scientificName', 'taxonKey'])
         self.openInputOutput()
         # Pull the header row 
         line, recno = self.getLine(self._iCsvrdr, 0)
         while (line is not None):
            # Get interpreted record
            line, recno = self.getLine(self._iCsvrdr, recno)
            if line is None:
               break
            # Create new record
            byline = self.createBisonLine(line)
            if byline:
               self._outWriter.writerow(byline)
      except Exception, e:
         self._log.error('Failed on line {}, exception {}'.format(recno, e))
      finally:
         self.close()

# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Parse a GBIF occurrence dataset downloaded
                            from the GBIF occurrence web service in
                            Darwin Core format into BISON format.  
                         """))
   parser.add_argument('infile', type=str, 
                       help='The full pathname of the input GBIF occurrence download file')
   args = parser.parse_args()
   interpFname = args.infile

   if os.path.exists(interpFname):
      if os.path.exists(interpFname):
         pth, basefname = os.path.split(interpFname)
         metaFname = os.path.join(pth, META_FNAME)
         outFname = os.path.join(pth, OUT_BISON + CSV_EXT)
         
         gr = GBIFReader(interpFname, metaFname, outFname)
         gr.extractData()
      else:
         print('Filename {} does not exist'.format(interpFname))

#    parser.add_argument('--start_line', type=int, default=1,
#             help=('First line from original file in dataset'))
#    parser.add_argument('--stop_line', type=int, default=1000000,
#             help=('Last line from original file in dataset'))
#    args = parser.parse_args()
#    first = args.start_line
#    last = args.stop_line

#    subdir = SUBDIRS[0]
#    postfix = '{}{}-{}'.format(SUBSET_PREFIX, first, last)
#    interpFname = os.path.join(DATAPATH, subdir, INTERPRETED + postfix + CSV_EXT)
#    if os.path.exists(interpFname):
#       verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM + CSV_EXT)
#       outFname = os.path.join(DATAPATH, subdir, OUT_BISON + postfix + CSV_EXT)
#       datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR) 
#       
#       gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)
#       gr.extractData()
# #       gr.testExtract()
#    else:
#       print('Filename {} does not exist'.format(interpFname))
   
   
"""
# python /state/partition1/workspace/bison/src/gbif/gbif2bison.py 7500000 8000000
import os
import xml.etree.ElementTree as ET

from src.gbif.gbif2bison import *
from src.gbif.constants import *
from src.gbif.gbifapi import GBIFCodes
from src.gbif.tools import *

from src.gbif.gbif2bison import *

subdir = SUBDIRS[0]
postfix = '{}{}-{}'.format(SUBSET_PREFIX, first, last)
interpFname = os.path.join(DATAPATH, subdir, INTERPRETED + postfix + CSV_EXT)
verbatimFname = os.path.join(DATAPATH, subdir, VERBATIM + CSV_EXT)
outFname = os.path.join(DATAPATH, subdir, OUT_BISON + postfix + CSV_EXT)
datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR) 

gr = GBIFReader(verbatimFname, interpFname, META_FNAME, outFname, datasetPath)
gr.openInterpreted()

fm = gr.fldMeta
line, recno = gr.getLine(gr._iCsvrdr, 0)
while (line is not None):
   line, recno = gr.getLine(gr._iCsvrdr, recno)
   if line is None:
      break
   byline = gr.createBisonLine(line)
   if byline:
      gr._outWriter.writerow(byline)      


gr.close()



import os
import xml.etree.ElementTree as ET

fname = 'fe7e89d1-8039-43dc-a833-df08f05f36b7.xml'
tree = ET.parse(fname)
root = tree.getroot()
ds  = root.find('dataset')
title = ds.find('title').text

"""
   
   
   
         
