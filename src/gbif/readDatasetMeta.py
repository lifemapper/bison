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
from gbifapi import GBIFCodes
from tools import openForReadWrite, getLogger
      
# .............................................................................
class GBIFMetaReader(object):
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
   def __init__(self, outProviders, outDatasets, datasetPath):
      """
      @summary: Constructor
      @param outFname: Full filename for the output lookup file for datasets 
      @param datasetPath: Directory containing individual files describing 
             datasets referenced in occurrence records.
      """
      self.gbifRes = GBIFCodes()
      self._files = []
      
      # Path to GBIF provided dataset metadata files
      self._datasetPath = datasetPath
      
      # Output metadata file
      self.outFname = outFname
      self._outf = None
      self._files.append(self._outf)
      self._outWriter = None
      pth, outbasename = os.path.split(outFname)
      outbase, ext = os.path.splitext(outbasename)
      
      # Input for dataset and provider UUID lookup
      self.datasetLookup = {}
      self.providerLookup = {}

      logname, _ = os.path.splitext(os.path.basename(__file__))
      logfname = os.path.join(pth, outbase + '.log')
      if os.path.exists(logfname):
         os.remove(logfname)
      self._log = getLogger(logname, logfname)
      
   # ...............................................

 
   # ...............................................
   def getDatasetTitle(self, uuid):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      fname = os.path.join(self._datasetPath, '{}.txt'.format(uuid))
      tree = ET.parse(fname)
      root = tree.getroot()
      ds  = root.find('dataset')
      title = ds.find('title').text
      return title
      
   # ...............................................
   def getDatasetInfo(self, fname):
      '''
      @summary: Read metadata for dataset with this uuid
      '''
      fullfname = os.path.join(self._datasetPath, fname)
      try:
         tree = ET.parse(fname)
         root = tree.getroot()
         ds  = root.find('dataset')
         title = ds.find('title').text
         
      except Exception, e:
         self._log.error('Failed parsing {}, exception {}'.format(fullfname, e))

   # ...............................................
   def extractDatasetMetadata(self):
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
         
      datasets = {}
      
      lookupDict, outWriter, outfile = openForReadWrite(fname, numKeys=1, numVals=1, header=None, 
                     delimiter=DELIMITER)

      fnames = glob.glob('*.xml')
      for fname in fnames:
         uuid = os.path.splitext(fname)
         dinfo = self.getDatasetInfo(fname)
         datasets[uuid] = dinfo
   

   # ...............................................
   def extractProviderMetadata(self):
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
         
      datasets = {}
      
      lookupDict, outWriter, outfile = openForReadWrite(fname, numKeys=1, numVals=1, header=None, 
                     delimiter=DELIMITER)

      fnames = glob.glob('*.xml')
      for fname in fnames:
         uuid = os.path.splitext(fname)
         dinfo = self.getDatasetInfo(fname)
         datasets[uuid] = dinfo
   


# ...............................................
if __name__ == '__main__':
   outDatasets = os.path.join(DATAPATH, 'datasetMeta.csv')
   outProviders = os.path.join(DATAPATH, 'providerMeta.csv')
   for subdir in SUBDIRS:      
      datasetPath = os.path.join(DATAPATH, subdir, DATASET_DIR) 
      dr = GBIFMetaReader(outDatasets, outProviders, datasetPath)
      
      dr.extractData()
   
   
"""

import os
import xml.etree.ElementTree as ET

fname = 'fe7e89d1-8039-43dc-a833-df08f05f36b7.xml'
tree = ET.parse(fname)
root = tree.getroot()
ds  = root.find('dataset')
title = ds.find('title').text

"""
   
   
   
         
