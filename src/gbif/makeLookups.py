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
   def __init__(self, filepath):
      """
      @summary: Constructor
      @param filepath: Path for input and output datafiles
      """
      self.fullpath = filepath
      self.gbifRes = GBIFCodes()
      self._files = []
            
      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(filepath, logname + '.log')
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
   def extractDatasetMetadata(self, resourceFname, uuidList):
      """
      @summary: Create a CSV file containing GBIF dataset metadata  
                extracted from the GBIF API, for only datasets with UUIDs 
                in the DS_UUID_FNAME..
      @return: A CSV file of metadata records for GBIF datasets. 
      """
      self.gbifRes.getDatasetCodes(resourceFname, uuidList)
   
   # ...............................................
   def extractProviderMetadata(self, provFname):
      """
      @summary: Create a CSV file containing GBIF organizations metadata 
                extracted from the GBIF API
      @return: A CSV file of metadata records for GBIF organizations. 
      """
      self.gbifRes.getProviderCodes(provFname)
      
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
   def _writeGBIFParserInput(self, inNameIdFname, outScinameFname):
      '''
      @summary: Read scientificName, taxonKey(s) from input CSV file, write 
                a JSON file with scientificName list for input to the 
                GBIF parser.
      @param inNameIdFname: Input CSV file with scientificName, and one or more
                            taxonKeys (identifier for GBIF taxonomic record).
      @param outScinameFname: Output JSON file with list of ScientificNames 
                            for parsing by the GBIF parser.
      '''
      try:
         scif = open(outScinameFname, 'wb')
         scif.write('[{}'.format(NEWLINE))
         csvreader, inf = getCSVReader(inNameIdFname, DELIMITER)
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
   def parseScientificNames(self, nameIdBasename):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      nameIdFname = os.path.join(self.fullpath, nameIdBasename)
      if not os.path.exists(nameIdFname):
         self._log.error('File {} does not exist for parsing scientific names')
         return 

      spfullbasename, _ = os.path.splitext(nameIdFname)
      inScinameJSON = spfullbasename + '_sciname.json'
      outScinameCannameCSV = spfullbasename + '_sciname_canname.csv'

      if not os.path.exists(inScinameJSON):
         self._writeGBIFParserInput(nameIdFname, inScinameJSON)
      self.gbifRes.parseScientificListFromFile(inScinameJSON, 
                                               outScinameCannameCSV)

# ...............................................
def concatenateLookups(filepath, outfname, pattern=None, fnames=None):
   '''
   @summary: Concatenate named files or files matching pattern into a single file. 
   @param filepath: Pathname to input files
   @param outfname: Basename of output file
   @param pattern: Pattern to match for input files
   @param fnames: Basename of one or more input file
   '''
   outfname = os.path.join(filepath, outfname)
   infnames = []
   try:
      csvwriter, outf = getCSVWriter(outfname, DELIMITER)

      if pattern is not None:
         infnames = glob.glob(os.path.join(filepath, pattern))
      if fnames is not None:
         for fn in fnames:
            infnames.append(os.path.join(filepath, fn))            

      for fname in infnames:
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

      
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Parse GBIF provided dataset metadata files, 
                            Request and parse provider metadata from GBIF API,
                            Request parsing of a file of species names, or request
                            and parse results from GBIF species API for taxonkey.
                         """))   
   parser.add_argument('--file_path', type=str, default=None,
                       help="Full path to input data.")
   parser.add_argument('--name_file', type=str, default=None,
                       help="""
                       Base filename of the input file containing 
                       scientificName and taxonKey(s) for names to be resolved.
                       """)
   parser.add_argument('--big_name_file', type=str, default=None,
                       help="""
                       Base filename of the VERY LARGE input file to be split
                       into smaller files.
                       """)
   parser.add_argument('--do_provider', type=bool, default=False,
                       help="""
                       Query GBIF API to create an OrganizationUUID-provider 
                       lookup file.
                       """)
   parser.add_argument('--do_resource', type=bool, default=False,
                       help="""
                       Query GBIF API to create a DatasetUUID-resource
                       lookup file.
                       """)
   parser.add_argument('--do_concatenate', type=bool, default=False,
                       help="""
                       Concatenate canonical name lookup files into a single 
                       large lookup file.
                       """)
   args = parser.parse_args()
   filepath = args.file_path
   nameIdBasename = args.name_file
   bigNameIdBasename = args.big_name_file
   doProvider = args.do_provider
   doResource = args.do_resource
   doConcatenate = args.do_concatenate
   
   if os.path.isdir(filepath):
      print('Error: Filepath {} does not exist'.format(filepath))
   else:
      gmr = GBIFMetaReader(filepath)
      
      # Split files into smaller chunks
      if bigNameIdBasename:
         bigNameIdFname = os.path.join(filepath, bigNameIdBasename)
         if os.path.exists(bigNameIdFname):
            splitFile(bigNameIdFname, limit=500)
   
      # Concatenate name lookup files into single file
      elif doConcatenate:
         fnames = []
         fnames = ['canonicalLookup_2s.csv', 'canonicalLookup.csv']
         concatenateLookups(filepath, 'canonicalLookup_us.csv', 
#                             pattern='',
                            fnames=fnames)
         
      # Canonical name from Scientific Name
      elif nameIdBasename:
         #  1. Create JSON Scientific name file for GBIF Parser input 
         #  2. Create CSV ScientificName to CanonicalName lookup file from GBIF Parser 
         gmr.parseScientificNames(nameIdBasename)
   
      elif doProvider:
         # Create organization UUID to provider lookup table
         gmr.extractProviderMetadata('providerLookup.csv')
         
      elif doResource:
         # Create dataset UUID to resource lookup table (only datasets with EML files)
         datasetPath = os.path.join(filepath, 'dataset')
         uuids = set()       
         if os.path.exists(datasetPath):
            for line in open(DS_UUID_FNAME, 'r'):
               stp = line.rfind('.xml')
               uuid = line[:stp]
               uuids.add(uuid)   
         gmr.extractDatasetMetadata('resourceLookup.csv', uuids)          
      
"""
python /state/partition1/workspace/bison/src/gbif/makeLookups.py \
       --file_path /state/partition1/data/bison/us \
       --name_file /nameUUIDForLookup_2_2.csv  
"""