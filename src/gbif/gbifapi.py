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
import json
import urllib2

from constants import DELIMITER, GBIF_URL, ENCODING
        
inProvFname = 'inProviderIDs.txt'
outProvFname = 'outProviderRecs.txt'

outDSFname = 'outDatasetRecs.txt'

# outProvFname = 'outProviderRecs.txt'
# inProvFname = 'inProviderIDs.txt'
# .............................................................................
class GBIFCodes(object):
   """
   @summary: Pulls UUIDs and metadata for local resolution of 
             GBIF Organizations, Providers, Resources
   """
# ...............................................
   def __init__(self):
      pass
   
# ...............................................
   def _saveNL(self, strval):
      fval = strval.replace("\n", "\\n")
      return fval

# ...............................................
   def _saveNLDelCR(self, strval):
      fval = strval.replace("\n", "\\n").replace("\r", "")
      return fval

# ...............................................
   def resolveDatasetKey(self, dsKey):
      row = []
      url = '{}/dataset/{}'.format(GBIF_URL, dsKey)
      response = urllib2.urlopen(url)
      data = json.load(response)
      legacyid = 'NA'
      orgkey = title = desc = citation = rights = logo = homepage = logo = ''
      if data.has_key('key'):
         if data.has_key('publishingOrganizationKey'):
            orgkey = self._saveNLDelCR(data['publishingOrganizationKey'])

         if (data.has_key('identifiers') 
             and len(data['identifiers']) > 0 
             and data['identifiers'][0]['type'] == 'GBIF_PORTAL'):
            legacyid = data['identifiers'][0]['identifier']
         
         if data.has_key('title'):
            title = self._saveNLDelCR(data['title'])            

         if data.has_key('description'):
            desc = self._saveNLDelCR(data['description'])
         
         if data.has_key('citation'):
            citation = self._saveNLDelCR(data['citation']['text'])
            
         if data.has_key('rights'):
            rights = self._saveNLDelCR(data['rights'])

         if data.has_key('logoUrl'):
            logourl = data['logoUrl']
         
         if data.has_key('homepage'):
            homepage = data['homepage']

         row = [orgkey, legacyid, data['key'], title, desc, citation, rights, logourl,
                data['created'], data['modified'], homepage]
      return row

# ...............................................
   def getProviderCodes(self):
      header = ['legacyid', 'key', 'title', 'description', 'created', 
                'modified', 'homepage']
      legacyid = 9999
      desc = homepage = ''
      url = '{}/organization?limit=9999'.format(GBIF_URL)
      response = urllib2.urlopen(url)
      data = json.load(response)
      allProvs = data['results']
      print 'Recieved {} organizations from GBIF'.format(len(allProvs))
      try:
         outf = codecs.open(outProvFname, 'w', ENCODING)
         inf = codecs.open(inProvFname, 'w', ENCODING)
         outf.write(DELIMITER.join(header))

         recno = 0
         for prov in allProvs:
            recno += 1
            row = []
            if (prov.has_key("identifiers") 
                and len(prov['identifiers']) > 0 
                and prov['identifiers'][0]['type'] == 'GBIF_PORTAL'):
               outLegacyid = prov['identifiers'][0]['identifier']
               inf.write(outLegacyid + '\n')
               legacyid = self._saveNLDelCR(outLegacyid)
      
            if prov.has_key("description"):
               desc = self._saveNLDelCR(prov['description'])
               
            if prov.has_key("homepage"):
               homepageVal= prov['homepage']
               if type(homepageVal) is list and len(homepageVal) > 0:
                  homepageVal = self._saveNLDelCR(prov['homepage'][0])
               elif type(homepageVal) is str:
                  homepageVal = self._saveNLDelCR(prov['homepage'])

            row = [legacyid, prov['key'], prov['title'], desc, prov['created'],
                   prov['modified'], homepage]
            
            outf.write(DELIMITER.join(row))
               
      finally:
         outf.close()
         inf.close()

# ...............................................
   def getDatasetCodes(self):
      header = ['publishingOrganizationKey', 'key', 'title', 'description', 
                'citation', 'rights', 'logoUrl', 'created', 'modified', 
                'homepage']
#       header = ['owningorganization_id', 'legacyid', 'dataset_id', 'name', 
#                 'description', 'citation', 'created', 'modified', 'website_url']
      orgkey = title = rights = logo = desc = homepage = ''
      url = '{}/dataset?limit=9999'.format(GBIF_URL)
      response = urllib2.urlopen(url)
      data = json.load(response)
      allDatasets = data['results']
      print 'Received {} datasets from GBIF'.format(len(allDatasets))
      try:
         outf = codecs.open(outDSFname, 'w', ENCODING)
         outf.write(DELIMITER.join(header))

         recno = 0
         for ds in allDatasets:
            recno += 1
            row = []
            if ds.has_key("title"):
               title = self._saveNLDelCR(ds['title'])
            if ds.has_key("rights"):
               rights = self._saveNLDelCR(ds['rights'])
            if ds.has_key("logoUrl"):
               logo = self._saveNLDelCR(ds['logoUrl'])
            if ds.has_key("description"):
               desc = self._saveNLDelCR(ds['description'])
               
            if ds.has_key("homepage"):
               homepageVal= ds['homepage']
               if type(homepageVal) is list and len(homepageVal) > 0:
                  homepageVal = self._saveNLDelCR(ds['homepage'][0])
               elif type(homepageVal) is str:
                  homepageVal = self._saveNLDelCR(ds['homepage'])
            
            row = [orgkey, ds['key'], title, desc, ds['citation']['text'], 
                   rights, logo, ds['created'], ds['modified'], homepage]
            outf.write(DELIMITER.join(row))
               
      finally:
         outf.close()

# ...............................................
   def resolveProviderKey(self, prvKey):
      row = []
      url = '{}/organization/{}'.format(GBIF_URL, prvKey)
      response = urllib2.urlopen(url)
      data = json.load(response)
      legacyid = 'NA'
      orgkey = title = desc = citation = rights = logourl = homepage = ''
      if data.has_key('key'):
         if data.has_key('publishingOrganizationKey'):
            orgkey = self._saveNLDelCR(data['publishingOrganizationKey'])

         if (data.has_key('identifiers') 
             and len(data['identifiers']) > 0 
             and data['identifiers'][0]['type'] == 'GBIF_PORTAL'):
            legacyid = data['identifiers'][0]['identifier']
         
         if data.has_key('title'):
            title = self._saveNLDelCR(data['title'])            

         if data.has_key('description'):
            desc = self._saveNLDelCR(data['description'])
         
         if data.has_key('citation'):
            citation = self._saveNLDelCR(data['citation']['text'])
            
         if data.has_key('rights'):
            rights = self._saveNLDelCR(data['rights'])

         if data.has_key('logoUrl'):
            logourl = data['logoUrl']
         
         if data.has_key('homepage'):
            homepage = data['homepage']

         row = [orgkey, legacyid, data['key'], title, desc, citation, rights, logourl,
                data['created'], data['modified'], homepage]
      return row

# ...............................................
   def resolveTaxonKey(self, taxKey):
      row = []
      canName = None
      url = '{}/species/{}'.format(GBIF_URL, taxKey)
      response = urllib2.urlopen(url)
      data = json.load(response)
      if data.has_key('canonicalName'):
         canName = data['canonicalName']
         row = [taxKey, canName, data['datasetKey']]
      return canName
#       return row

# ...............................................
if __name__ == '__main__':
   gc = GBIFCodes()
   gc.getProviderCodes()
   
"""
import csv
import codecs
import cStringIO
import json
import urllib2

from src.gbif.constants import *
from src.gbif.gbifresolve import *

gc = GBIFCodes()


"""