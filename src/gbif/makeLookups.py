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

from constants import DELIMITER
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
   def __init__(self, provFname, resFname, canFname, inSpeciesFname, datasetPath):
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
      
      # Path to GBIF provided dataset metadata files
      self._datasetPath = datasetPath
      
      # Output lookup files
      self.provFname = provFname
      
      self.resFname = resFname
      self._resf = None
      self._files.append(self._resf)
      self._resWriter = None

      self.canFname = canFname
      self._canf = None
      self._files.append(self._canf)
      self._canWriter = None
      
      self.inSpeciesFname = inSpeciesFname
      self._inf = None
      self._files.append(self._inf)
      self._speciesReader = None
      
      pth, outbasename = os.path.split(provFname)
      
      # Input for dataset and provider UUID lookup
      self.datasetLookup = {}
      self.providerLookup = {}

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
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=("""Parse GBIF provided dataset metadata files, 
                            Request and parse provider metadata from GBIF API,
                            Request parsing of a file of species names, or request
                            and parse results from GBIF species API for taxonkey.
                         """))
   parser.add_argument('name_id_input_file', type=str, 
                       help="""
                       Full pathname of the input file containing 
                       scientificName and taxonKey(s) for names to be resolved.
                       """)
   parser.add_argument('dataset_path', type=str, 
                       help=""""
                       The pathname for direcxtory containing GBIF dataset 
                       metadata files.
                       """)
   parser.add_argument('--names_only', type=bool, default=False,
            help=('Re-read a bison output file to retrieve scientificName and taxonID.'))
   args = parser.parse_args()
   nameIdFname = args.name_id_input_file
   datasetPath = args.dataset_path
   
   if os.path.exists(nameIdFname):
      pth, basefname = os.path.split(nameIdFname)
      
      provFname = os.path.join(pth, 'providerLookup.csv')
      resFname = os.path.join(pth, 'resourceLookup.csv')
      canFname = os.path.join(pth, 'canonicalLookup.csv')
      
      gmr = GBIFMetaReader(provFname, resFname, canFname, nameIdFname, datasetPath)
      print('Calling program with input/output {}'.format(nameIdFname, datasetPath))
      gmr.extractDatasetMetadata()
#       gmr.extractProviderMetadata()
   else:
      print('Filename {} does not exist'.format(nameIdFname))
   
"""

import os
import xml.etree.ElementTree as ET

fname = 'fe7e89d1-8039-43dc-a833-df08f05f36b7.xml'
tree = ET.parse(fname)
root = tree.getroot()
ds  = root.find('dataset')
title = ds.find('title').text

"""
   
   
   
         
