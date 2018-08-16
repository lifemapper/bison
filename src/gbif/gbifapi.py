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
import os
import urllib2

from constants import (DELIMITER, GBIF_URL, ENCODING, URL_ESCAPES, 
                       DATAPATH, OUT_PROVIDER, OUT_DATASET)
        
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
   def resolveDataset(self, dsKey):
      dataDict = {}
      # returns GBIF dataset
      if dsKey == '':
         return dataDict
      
      url = '{}/dataset/{}'.format(GBIF_URL, dsKey)
      try:
         response = urllib2.urlopen(url)
      except Exception, e:
         print('Failed to resolve URL {}'.format(url))
         return dataDict

      data = json.load(response)
      legacyid = 'NA'
      orgkey = title = desc = citation = rights = logourl = homepage = ''
      if data.has_key('key'):
         if data.has_key('publishingOrganizationKey'):
            orgkey = self._saveNLDelCR(data['publishingOrganizationKey'])
            dataDict['publishingOrganizationKey'] = orgkey

         if (data.has_key('identifiers') 
             and len(data['identifiers']) > 0 
             and data['identifiers'][0]['type'] == 'GBIF_PORTAL'):
            legacyid = data['identifiers'][0]['identifier']
            dataDict['legacyID'] = legacyid
         
         if data.has_key('title'):
            title = self._saveNLDelCR(data['title'])
            dataDict['title'] = title

         if data.has_key('description'):
            desc = self._saveNLDelCR(data['description'])
            dataDict['description'] = desc
         
         if data.has_key('citation'):
            citation = self._saveNLDelCR(data['citation']['text'])
            dataDict['citation'] = citation
            
         if data.has_key('rights'):
            rights = self._saveNLDelCR(data['rights'])
            dataDict['rights'] = rights

         if data.has_key('logoUrl'):
            logourl = data['logoUrl']
            dataDict['logoUrl'] = logourl
         
         if data.has_key('homepage'):
            homepage = data['homepage']
            dataDict['homepage'] = homepage
            
         dataDict['key'] = data['key']
         dataDict['created'] = data['created']
         dataDict['modified'] = data['modified']

#             row = [orgkey, legacyid, data['key'], title, desc, citation, rights, 
#                    logourl, data['created'], data['modified'], homepage]
      return dataDict

# ...............................................
   def getProviderFromDatasetKey(self, dsKey):
      row = self.resolveDatasetKey(dsKey)
      try:
         publishingOrgUUID = row[0]
      except:
         print('No record for datasetKey {}'.format(dsKey))
      return publishingOrgUUID

# ...............................................
   def _processProviderInfo(self, prov):
      desc = homepage = ''
      
      if prov.has_key("description"):
         desc = self._saveNLDelCR(prov['description'])
         
      if prov.has_key("homepage"):
         homepage= prov['homepage']
         if type(homepage) is list and len(homepage) > 0:
            homepage = self._saveNLDelCR(prov['homepage'][0])
         elif type(homepage) is str:
            homepage = self._saveNLDelCR(prov['homepage'])

      row = [prov['key'], prov['title'], desc, prov['created'],
             prov['modified'], homepage]
      
      return row
               
# ...............................................
   def _processDatasetInfo(self, ds):
      orgkey = title = desc = rights = logo = homepage = None 
           
      if ds.has_key("title"):
         title = self._saveNLDelCR(ds['title'])
      if ds.has_key("rights"):
         rights = self._saveNLDelCR(ds['rights'])
      if ds.has_key("logoUrl"):
         logo = self._saveNLDelCR(ds['logoUrl'])
      if ds.has_key("description"):
         desc = self._saveNLDelCR(ds['description'])
         
      if ds.has_key("homepage"):
         homepage= ds['homepage']
         if type(homepage) is list and len(homepage) > 0:
            homepage = self._saveNLDelCR(ds['homepage'][0])
         elif type(homepage) is str:
            homepage = self._saveNLDelCR(ds['homepage'])
      
      row = [orgkey, ds['key'], title, desc, ds['citation']['text'], 
             rights, logo, ds['created'], ds['modified'], homepage]
      
      return row


# ...............................................
   def getCodesFromAPI(self, apitype, outfname, header):
      offset = 0
      pagesize = 1000
      url = '{}/{}?offset={}&limit={}'.format(GBIF_URL, apitype, offset, pagesize)
      response = urllib2.urlopen(url)
      data = json.load(response)
      pcount = data['count']
      allObjs = data['results']
      isComplete = data['endOfRecords']
      total = len(allObjs)
      
      while (not isComplete and total < pcount):  
         print 'Received {} {}s from GBIF'.format(len(allObjs), apitype)
         try:
            outf = codecs.open(outfname, 'w', ENCODING)
            outf.write(DELIMITER.join(header))

            recno = 0
            for obj in allObjs:
               recno += 1
               if apitype == 'organization':
                  row = self._processProviderInfo(obj)
               elif apitype == 'dataset':
                  row = self._processDatasetInfo(obj)
               else:
                  print('What kind of query is {}?'.format(apitype))
                             
               outf.write(DELIMITER.join(row))

            url = '{}/{}?offset={}&limit={}'.format(GBIF_URL, apitype, offset, pagesize)
            response = urllib2.urlopen(url)
            data = json.load(response)
            pcount = data['count']
            allObjs = data['results']
            isComplete = data['endOfRecords']
            total += len(allObjs)
            
         except Exception, e:
            raise      
         finally:
            outf.close()


# ...............................................
   def getProviderCodes(self):
      header = ['key', 'title', 'description', 'created', 
                'modified', 'homepage']
      outProvFname = os.path.join(DATAPATH, OUT_PROVIDER)
      self.getCodesFromAPI('organization', outProvFname, header)

# ...............................................
   def getDatasetCodes(self):
      header = ['publishingOrganizationKey', 'key', 'title', 'description', 
                'citation', 'rights', 'logoUrl', 'created', 'modified', 
                'homepage']
      outDSFname = os.path.join(DATAPATH, OUT_DATASET)
      self.getCodesFromAPI('dataset', outDSFname, header)

# # ...............................................
#    def getProviderCodes(self):
#       header = ['key', 'title', 'description', 'created', 
#                 'modified', 'homepage']
#       offset = 0
#       pagesize = 1000
#       url = '{}/organization?offset={}&limit={}'.format(GBIF_URL, offset, pagesize)
#       response = urllib2.urlopen(url)
#       data = json.load(response)
#       pcount = data['count']
#       allProvs = data['results']
#       isComplete = data['endOfRecords']
#       total = len(allProvs)
#       
#       while (not isComplete and total < pcount):  
#          print 'Received {} organizations from GBIF'.format(len(allProvs))
#          try:
#             outf = codecs.open(outProvFname, 'w', ENCODING)
#             outf.write(DELIMITER.join(header))
# 
#             recno = 0
#             for prov in allProvs:
#                recno += 1
#                row = self._processProviderInfo(prov)               
#                outf.write(DELIMITER.join(row))
#          except Exception, e:
#             raise      
#          finally:
#             outf.close()

# # ...............................................
#    def getDatasetCodes(self):
#       header = ['publishingOrganizationKey', 'key', 'title', 'description', 
#                 'citation', 'rights', 'logoUrl', 'created', 'modified', 
#                 'homepage']
# #       header = ['owningorganization_id', 'legacyid', 'dataset_id', 'name', 
# #                 'description', 'citation', 'created', 'modified', 'website_url']
#       orgkey = title = rights = logo = desc = homepage = ''
#       url = '{}/dataset?limit=9999'.format(GBIF_URL)
#       response = urllib2.urlopen(url)
#       data = json.load(response)
#       allDatasets = data['results']
#       print 'Received {} datasets from GBIF'.format(len(allDatasets))
#       try:
#          outf = codecs.open(outDSFname, 'w', ENCODING)
#          outf.write(DELIMITER.join(header))
# 
#          recno = 0
#          for ds in allDatasets:
#             recno += 1
#             row = []
#             if ds.has_key("title"):
#                title = self._saveNLDelCR(ds['title'])
#             if ds.has_key("rights"):
#                rights = self._saveNLDelCR(ds['rights'])
#             if ds.has_key("logoUrl"):
#                logo = self._saveNLDelCR(ds['logoUrl'])
#             if ds.has_key("description"):
#                desc = self._saveNLDelCR(ds['description'])
#                
#             if ds.has_key("homepage"):
#                homepageVal= ds['homepage']
#                if type(homepageVal) is list and len(homepageVal) > 0:
#                   homepageVal = self._saveNLDelCR(ds['homepage'][0])
#                elif type(homepageVal) is str:
#                   homepageVal = self._saveNLDelCR(ds['homepage'])
#             
#             row = [orgkey, ds['key'], title, desc, ds['citation']['text'], 
#                    rights, logo, ds['created'], ds['modified'], homepage]
#             outf.write(DELIMITER.join(row))
#                
#       finally:
#          outf.close()


# ...............................................
   def resolveOrganization(self, orgUUID):
      # returns GBIF providerId
      dataDict = {}
      if orgUUID == '':
         return dataDict
      
      url = '{}/organization/{}'.format(GBIF_URL, orgUUID)
      try:
         response = urllib2.urlopen(url)
      except Exception, e:
         print('Failed to resolve URL {}'.format(url))
         return dataDict

      data = json.load(response)
      legacyid = 'NA'
      orgkey = title = desc = citation = rights = logourl = homepage = ''
      if data.has_key('key'):
         if data.has_key('publishingOrganizationKey'):
            orgkey = self._saveNLDelCR(data['publishingOrganizationKey'])
            dataDict['publishingOrganizationKey'] = orgkey

         if (data.has_key('identifiers') 
             and len(data['identifiers']) > 0 
             and data['identifiers'][0]['type'] == 'GBIF_PORTAL'):
            legacyid = data['identifiers'][0]['identifier']
            dataDict['legacyID'] = legacyid
         
         if data.has_key('title'):
            title = self._saveNLDelCR(data['title'])            
            dataDict['title'] = title

         if data.has_key('description'):
            desc = self._saveNLDelCR(data['description'])
            dataDict['description'] = desc
         
         if data.has_key('citation'):
            citation = self._saveNLDelCR(data['citation']['text'])
            dataDict['citation'] = citation

         if data.has_key('rights'):
            rights = self._saveNLDelCR(data['rights'])
            dataDict['rights'] = rights

         if data.has_key('logoUrl'):
            logourl = data['logoUrl']
            dataDict['logoUrl'] = logourl
         
         if data.has_key('homepage'):
            homepage = data['homepage']
            dataDict['homepage'] = homepage
            
         dataDict['key'] = data['key']
         dataDict['created'] = data['created']
         dataDict['modified'] = data['modified']

#          row = [orgkey, legacyid, data['key'], title, desc, citation, rights, logourl,
#                 data['created'], data['modified'], homepage]
      return dataDict

# ...............................................
   def resolveCanonicalFromTaxonKey(self, taxKey):
      canName = None
      url = '{}/species/{}'.format(GBIF_URL, taxKey)
      response = urllib2.urlopen(url)
      data = json.load(response)
      if data.has_key('canonicalName'):
         canName = data['canonicalName']
         row = [taxKey, canName, data['datasetKey']]
      return canName

# ...............................................
   def resolveCanonicalFromScientific(self, sciname):
      canName = None
      for replaceStr, withStr in URL_ESCAPES:
         sciname = sciname.replace(replaceStr, withStr)
      url = '{}/parser/name?name={}'.format(GBIF_URL, str(sciname))
      try:
         response = urllib2.urlopen(url)
      except Exception, e:
         print ('Failed to resolve name with {}'.format(url))
      else:
         data = json.load(response)
         if type(data) is list and len(data) > 0:
            data = data[0]
         if data.has_key('canonicalName'):
            canName = data['canonicalName']
      return canName

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
header = ['legacyid', 'key', 'title', 'description', 'created', 
          'modified', 'homepage']
offset = 0
pagesize = 1000
legacyid = 9999
desc = homepage = ''
url = '{}/organization?offset={}&limit={}'.format(GBIF_URL, offset, pagesize)
response = urllib2.urlopen(url)
data = json.load(response)
total = data['count']
allProvs = data['results']
isComplete = data['endOfRecords']


"""