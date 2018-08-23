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

from constants import DELIMITER, NEWLINE
from gbifapi import GBIFCodes
from tools import getCSVReader, getCSVWriter, getLogger

   
DS_UUID_FNAME = '/Users/astewart/git/lifemapper/bison/src/gbif/datasetUUIDs.txt'
# .............................................................................
class GBIFMetaReader(object):
   """
   @summary: Get or Read supporting GBIF metadata for organizations (BISON 
             provider), datasets (BISON resource), and species names. 
   @note: 
   """
   # ...............................................
   def __init__(self, provFname, resFname, canFname, inSpeciesFname=None, datasetPath=None):
      """
      @summary: Constructor
      @param provFname: Full filename for the output lookup file for organization/provider
      @param resFname: Full filename for the output lookup file for dataset/resource
      @param canFname: Full filename for the output lookup file for canonical names 
      @param datasetPath: Directory containing individual files describing 
             datasets referenced in occurrence records.
      """
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

      self.canFname = canFname
      self._canf = None
      self._files.append(self._canf)
      self._canWriter = None
      
      self.inSpeciesFname = inSpeciesFname
      if inSpeciesFname is None:
         self.outSciFname = None
      else:
         spfullbasename, _ = os.path.splitext(inSpeciesFname)
         self.outSciFname = spfullbasename + '_sciname_.txt'
         
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
   def extractDatasetMetadata(self):
      """
      @summary: Create a CSV file containing GBIF dataset metadata  
                extracted from the GBIF API, for only datasets with UUIDs 
                in the DS_UUID_FNAME..
      @return: A CSV file of metadata records for GBIF datasets. 
      """
      uuids = set()
      for line in open(DS_UUID_FNAME, 'r'):
         stp = line.rfind('.xml')
         uuid = line[:stp]
         uuids.add(uuid)
         
      self.gbifRes.getDatasetCodes(self.resFname, uuids)
   
   # ...............................................
   def extractProviderMetadata(self):
      """
      @summary: Create a CSV file containing GBIF organizations metadata 
                extracted from the GBIF API
      @return: A CSV file of metadata records for GBIF organizations. 
      """
      self.gbifRes.getProviderCodes(self.provFname)

   # ...............................................
   def _writeGBIFParserInput(self):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      if os.path.exists(self.outSciFname):
         self._log.info('Scientific name list file {} already exists'
                        .format(self.outSciFname))
         return
      try:
         csvwriter, scif = getCSVWriter(self.outSciFname, DELIMITER, doAppend=False)
#          scif = open(self.outSciFname, 'w')
         csvreader, inf = getCSVReader(self.inSpeciesFname, DELIMITER)
         while csvreader is not None:
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
                              .format(csvreader.line, e))
            else:
               csvwriter.writerow([sciname])
      finally:
         scif.close()
         inf.close()

   # ...............................................
   def parseSciNames(self):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      self._writeGBIFParserInput()


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
#    parser.add_argument('--names_only', type=bool, default=False,
#             help=('Re-read a bison output file to retrieve scientificName and taxonID.'))
   args = parser.parse_args()
   nameIdFname = args.name_file
   datasetPath = args.dataset_path
   
   if os.path.exists(nameIdFname):
      pth, basefname = os.path.split(nameIdFname)
      
      provFname = os.path.join(pth, 'providerLookup.csv')
      resFname = os.path.join(pth, 'resourceLookup.csv')
      canFname = os.path.join(pth, 'canonicalLookup.csv')
      
      gmr = GBIFMetaReader(provFname, resFname, canFname, inSpeciesFname=nameIdFname, datasetPath=datasetPath)
      print('Calling program with input/output {}'.format(nameIdFname, datasetPath))
      if nameIdFname is not None:
         gmr.parseSciNames()
      if datasetPath is not None:
         gmr.extractDatasetMetadata()
#       gmr.extractProviderMetadata()
   else:
      print('Filename {} does not exist'.format(nameIdFname))
   
"""
python makeLookups.py /state/partition1/data/bison/us/nameUUIDForLookup_1-10000000.csv  /state/partition1/data/bison/us/dataset



import os
import xml.etree.ElementTree as ET

fname = 'fe7e89d1-8039-43dc-a833-df08f05f36b7.xml'
tree = ET.parse(fname)
root = tree.getroot()
ds  = root.find('dataset')
title = ds.find('title').text

"""
   
   
   
         
