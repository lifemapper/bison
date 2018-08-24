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
import codecs
import glob
import os
import xml.etree.ElementTree as ET

from constants import DELIMITER, NEWLINE, ENCODING
from gbifapi import GBIFCodes
from tools import getCSVReader, getCSVWriter, getLogger

   
DS_UUID_FNAME = '/state/partition1/data/bison/datasetUUIDs.txt'
# .............................................................................
class GBIFMetaReader(object):
   """
   @summary: Get or Read supporting GBIF metadata for organizations (BISON 
             provider), datasets (BISON resource), and species names. 
   @note: 
   """
   # ...............................................
   def __init__(self, provFname, resFname, nameIdFname=None, datasetPath=None):
      """
      @summary: Constructor
      @param provFname: Full filename for the output lookup file for organization/provider
      @param resFname: Full filename for the output lookup file for dataset/resource
      @param canFname: Full filename for the output lookup file for canonical names 
      @param datasetPath: Directory containing individual files describing 
             datasets referenced in occurrence records.
      """
      self.nameIdFname = nameIdFname
      self.scinameFname = None
      self.scinameCannameFname = None
      if nameIdFname is not None:
         spfullbasename, _ = os.path.splitext(nameIdFname)
         self.scinameFname = spfullbasename + '_sciname.json'
         self.scinameCannameFname = spfullbasename + '_sciname_canname.csv'

      self.gbifRes = GBIFCodes()
      self._files = []
            
      # Output lookup files
      self.provFname = provFname
      pth, _ = os.path.split(provFname)
      # Input for provider UUID lookup
      self.providerLookup = {}
      
      self.resFname = resFname
      self._resf = None
      self._files.append(self._resf)
      self._resWriter = None
      
         
      # Path to GBIF provided dataset metadata files
      self._datasetPath = datasetPath
      # Input for dataset UUID lookup
      self.datasetLookup = {}

      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(pth, logname + '.log')
      if os.path.exists(logfname):
         os.remove(logfname)
      self._log = getLogger(logname, logfname)
      
   # ...............................................
   def _getDatasetInfo(self, fname):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      try:
         tree = ET.parse(fname)
         root = tree.getroot()
         ds  = root.find('dataset')
         title = ds.find('title').text
         
      except Exception, e:
         self._log.error('Failed parsing {}, exception {}'.format(fname, e))

   # ...............................................
   def extractDatasetMetadata(self, uuidList):
      """
      @summary: Create a CSV file containing GBIF dataset metadata  
                extracted from the GBIF API, for only datasets with UUIDs 
                in the DS_UUID_FNAME..
      @return: A CSV file of metadata records for GBIF datasets. 
      """
      self.gbifRes.getDatasetCodes(self.resFname, uuidList)
   
   # ...............................................
   def extractProviderMetadata(self):
      """
      @summary: Create a CSV file containing GBIF organizations metadata 
                extracted from the GBIF API
      @return: A CSV file of metadata records for GBIF organizations. 
      """
      self.gbifRes.getProviderCodes(self.provFname)
      
   # ...............................................
   def _readData(self, csvreader):
      encSciname = None
      try:
         line = csvreader.next()
         if len(line) > 0:
            sciname = line[0]
      except OverflowError, e:
         self._log.info( 'Overflow on line {} ({})'
                         .format(csvreader.line, str(e)))
      except StopIteration:
         self._log.info('EOF after line {}'
                        .format(csvreader.line_num))
         csvreader = None
      except Exception, e:
         self._log.info('Bad record on line {} ({})'
                        .format(csvreader.line_num, e))
      else:
         encSciname = sciname.encode(ENCODING)
         
      return encSciname, csvreader
         
   # ...............................................
   def _writeData(self, outf, encodedString):
      # Write encoded data as binary
      try:
         outf.write('"{}"'.format(encodedString))
      except UnicodeDecodeError, e:
         self._log.error('Decode error {}'.format(e))
      except UnicodeEncodeError, e:
         self._log.error('Encode error {}'.format(e))
      

   # ...............................................
   def _writeGBIFParserInput(self):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      if os.path.exists(self.scinameFname):
         self._log.info('Deleting existing scientific name json file {}'
                        .format(self.scinameFname))
         os.remove(self.scinameFname)

      try:
         scif = open(self.scinameFname, 'wb')
         scif.write('[{}'.format(NEWLINE))
         csvreader, inf = getCSVReader(self.nameIdFname, DELIMITER)
         # discard header
         _, csvreader = self._readData(csvreader)
         # then get/write first line
         encSciname, csvreader = self._readData(csvreader)
         self._writeData(scif, encSciname)
         
         while csvreader is not None:
            encSciname, csvreader = self._readData(csvreader)

            if encSciname is not None:
               scif.write(',{}'.format(NEWLINE))
               self._writeData(scif, encSciname)

         scif.write('{}]{}'.format(NEWLINE, NEWLINE))
      finally:
         scif.close()
         inf.close()

   # ...............................................
   def createScinameInput(self):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      self._writeGBIFParserInput()
      
   # ...............................................
   def getGBIFParserOutput(self):
      self.gbifRes.parseScientificListFromFile(self.scinameFname, 
                                               self.scinameCannameFname)

# ...............................................
def concatenateLookups(pth, pattern, fnames=None):
   outfname = os.path.join(pth, 'canonicalLookup.csv')
   try:
      csvwriter, outf = getCSVWriter(outfname, DELIMITER)

      if fnames is None:
         fnames = glob.glob(os.path.join(pth, pattern))
      else:
         outfname = os.path.join(pth, 'canonicalLookup_v2.csv')

      for fname in fnames:
         csvreader, inf = getCSVReader(fname, DELIMITER)
         while csvreader is not None:
            try:
               line = csvreader.next()
            except OverflowError, e:
               print( 'Overflow on line {} ({})'.format(csvreader.line, str(e)))
            except StopIteration:
               print('EOF after line {}'.format(csvreader.line_num))
               csvreader = None
               inf.close()
            except Exception, e:
               print('Bad record on line {} ({})'.format(csvreader.line_num, e))
            else:
               csvwriter.writerow(line)
   except Exception, e:
      print('Failed in infile {}, {}'.format(fname, str(e)))
   finally:
      outf.close()
   
# ...............................................
def _getNextWriter(bigFname, currFnum):
   bigbasefname, ext  = os.path.splitext(bigFname)
   newfname = '{}_{}{}'.format(bigbasefname, currFnum, ext)
   csvwriter, outf = getCSVWriter(newfname, DELIMITER, doAppend=False)
   return csvwriter, outf
      
# ...............................................
def splitFile(bigFname, limit=50000):
   currFnum = 1
   stopLine = limit
   csvreader, inf = getCSVReader(bigFname, DELIMITER)
   csvwriter, outf = _getNextWriter(bigFname, currFnum)
   while csvreader is not None and csvreader.line_num < stopLine:
      try:
         line = csvreader.next()
      except OverflowError, e:
         print( 'Overflow on line {} ({})'.format(csvreader.line, str(e)))
      except StopIteration:
         print('EOF after line {}'.format(csvreader.line_num))
         csvreader = None
         inf.close()
      except Exception, e:
         print('Bad record on line {} ({})'.format(csvreader.line_num, e))
      else:
         csvwriter.writerow(line)
         
      if csvreader is None:
         outf.close()
      elif csvreader.line_num >= stopLine:
         outf.close()
         currFnum += 1
         csvwriter, outf = _getNextWriter(bigFname, currFnum)
         stopLine += limit
      
# ...............................................
def splitFile2(bigFname, limit=50000):
   currFnum = 2
   startLine = limit
   stopLine = startLine + limit
   csvreader, inf = getCSVReader(bigFname, DELIMITER)
   csvwriter, outf = _getNextWriter(bigFname, currFnum)
   while csvreader is not None and csvreader.line_num < stopLine:
      try:
         line = csvreader.next()
      except OverflowError, e:
         print( 'Overflow on line {} ({})'.format(csvreader.line, str(e)))
      except StopIteration:
         print('EOF after line {}'.format(csvreader.line_num))
         csvreader = None
         inf.close()
      except Exception, e:
         print('Bad record on line {} ({})'.format(csvreader.line_num, e))
      else:
         if csvreader.line_num > startLine and csvreader.line_num <= stopLine: 
            csvwriter.writerow(line)
         if csvreader.line_num >= stopLine:
            pass
            
   outf.close()
   csvreader = None
   inf.close()
         
      
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Parse GBIF provided dataset metadata files, 
                            Request and parse provider metadata from GBIF API,
                            Request parsing of a file of species names, or request
                            and parse results from GBIF species API for taxonkey.
                         """))   
   parser.add_argument('--name_file', type=str, default=None,
                       help="""
                       Full pathname of the input file containing 
                       scientificName and taxonKey(s) for names to be resolved.
                       """)
   parser.add_argument('--dataset_path', type=str, default=None, 
                       help=""""
                       The pathname for directory containing GBIF dataset 
                       metadata files.
                       """)
                       
   parser.add_argument('--big_name_file', type=str, default=None,
                       help="""
                       Full pathname of the VERY LARGE input file to be split
                       into smaller files.
                       """)
   parser.add_argument('--canonical_lookup_path', type=str, default=None, 
                       help=""""
                       The pathname for directory containing GBIF dataset 
                       metadata files.
                       """)
#    parser.add_argument('--names_only', type=bool, default=False,
#             help=('Re-read a bison output file to retrieve scientificName and taxonID.'))
   args = parser.parse_args()
   bigNameIdFname = args.big_name_file
   canonicalLookupPath = args.canonical_lookup_path
   nameIdFname = args.name_file
   datasetPath = args.dataset_path
   
   if bigNameIdFname and os.path.exists(bigNameIdFname):
      splitFile(bigNameIdFname)
#       splitFile2(bigNameIdFname)

   elif canonicalLookupPath is not None:
      fnames = [os.path.join(canonicalLookupPath, 'nameUUIDForLookup_2_sciname_canname.csv'),
                os.path.join(canonicalLookupPath, 'canonicalLookup.csv')]
      concatenateLookups(canonicalLookupPath, 
                         'nameUUIDForLookup*sciname_canname.csv',
                         fnames=fnames)
      
   elif nameIdFname and os.path.exists(nameIdFname):
      pth, basefname = os.path.split(nameIdFname)
      provFname = os.path.join(pth, 'providerLookup.csv')
      resFname = os.path.join(pth, 'resourceLookup.csv')
       
      gmr = GBIFMetaReader(provFname, resFname, nameIdFname=nameIdFname, datasetPath=datasetPath)
      print('Calling program with input/output {}'.format(nameIdFname, datasetPath))
      if nameIdFname is not None:
         if not(os.path.exists(gmr.scinameFname)):
            gmr.createScinameInput()
         if not(os.path.exists(gmr.scinameCannameFname)):
            gmr.getGBIFParserOutput()
          
      if datasetPath is not None:
               uuids = set()
      for line in open(DS_UUID_FNAME, 'r'):
         stp = line.rfind('.xml')
         uuid = line[:stp]
         uuids.add(uuid)
         

         gmr.extractDatasetMetadata()
      gmr.extractProviderMetadata()
   else:
      print('Filename {} does not exist'.format(nameIdFname))
   
"""
python /state/partition1/workspace/bison/src/gbif/makeLookups.py \
       --name_file /state/partition1/data/bison/us/nameUUIDForLookup_1-10000000.csv  



import os
import xml.etree.ElementTree as ET

fname = 'fe7e89d1-8039-43dc-a833-df08f05f36b7.xml'
tree = ET.parse(fname)
root = tree.getroot()
ds  = root.find('dataset')
title = ds.find('title').text

"""
   
   
   
         
