"""
@summary: Module containing functions for API Queries
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import json
import requests
from types import (BooleanType)
import urllib
import xml.etree.ElementTree as ET

from LmCommon.common.lmconstants import (GBIF, URL_ESCAPES, HTTPStatus)

# .............................................................................
class APIQuery(object):
   """
   Class to query APIs and return results
   """
   def __init__(self, baseurl, 
                qKey = None, qFilters={}, otherFilters={}, filterString=None, 
                headers={}):
      """
      @summary Constructor for the APIQuery class
      """
      self._qKey = qKey
      self.headers = headers
      # No added filters are on url (unless initialized with filters in url)
      self.baseurl = baseurl
      self._qFilters = qFilters
      self._otherFilters = otherFilters
      self.filterString = self._assembleFilterString(filterString=filterString)
      self.output = None
      self.debug = False
      
# ...............................................
   @classmethod
   def initFromUrl(cls, url, headers={}):
      base, filters = url.split('?')
      qry = APIQuery(base, filterString=filters)
      return qry
      
   # .........................................
   @property
   def url(self):
      # All filters added to url
      if self.filterString and len(self.filterString) > 1:
         return '{}?{}'.format(self.baseurl, self.filterString)
      else:
         return self.baseurl
      
# ...............................................
   def addFilters(self, qFilters={}, otherFilters={}):
      """
      @summary: Add new or replace existing filters.  This does not remove 
                existing filters, unless existing keys are sent with new values.
      """
      self.output = None
      for k, v in qFilters.iteritems():
         self._qFilters[k] = v
      for k, v in otherFilters.iteritems():
         self._otherFilters[k] = v
      self.filterString = self._assembleFilterString()
         
# ...............................................
   def clearAll(self, qFilters=True, otherFilters=True):
      """
      @summary: Clear existing qFilters, otherFilters, and output
      """
      self.output = None
      if qFilters:
         self._qFilters = {}
      if otherFilters:
         self._otherFilters = {}
      self.filterString = self._assembleFilterString()

# ...............................................
   def clearOtherFilters(self):
      """
      @summary: Clear existing otherFilters and output
      """
      self.clearAll(otherFilters=True, qFilters=False)

# ...............................................
   def clearQFilters(self):
      """
      @summary: Clear existing qFilters and output
      """
      self.clearAll(otherFilters=False, qFilters=True)

# ...............................................
   def _assembleFilterString(self, filterString=None):
      if filterString is not None:
         for replaceStr, withStr in URL_ESCAPES:
            filterString = filterString.replace(replaceStr, withStr)
      else:
         allFilters = self._otherFilters.copy()
         if self._qFilters:
            qVal = self._assembleQVal(self._qFilters)
            allFilters[self._qKey] = qVal
         filterString = self._assembleKeyValFilters(allFilters)
      return filterString

# ...............................................
   def _assembleKeyValFilters(self, ofDict):
      for k, v in ofDict.iteritems():
         if isinstance(v, BooleanType):
            v = str(v).lower()
         ofDict[k] = unicode(v).encode('utf-8')         
      filterString = urllib.urlencode(ofDict)
      return filterString
      

   
# ...............................................
   def queryByGet(self, outputType='json'):
      """
      @summary: Queries the API and sets 'output' attribute to a JSON object 
      """
      self.output = None
      data = retcode = None
      try:
         response = requests.get(self.url, headers=self.headers)
      except Exception, e:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            reason = 'Unknown Error'
         print('Failed on URL {}, code = {}, reason = {} ({})'
               .format(self.url, retcode, reason, str(e)))
       
      if response.status_code == 200:
         if outputType == 'json':
            try:
               self.output = response.json()
            except Exception, e:
               output = response.content
               self.output = deserialize(fromstring(output))
         elif outputType == 'xml':
            output = response.text
            self.output = deserialize(fromstring(output))
         else:
            print('Unrecognized output type {}'.format(outputType))
      else:
         print('Failed on URL {}, code = {}, reason = {}'
               .format(self.url, response.status_code, response.reason))

# ...............................................
   def queryByPost(self, outputType='json'):
      self.output = None
      allParams = self._otherFilters.copy()
      allParams[self._qKey] = self._qFilters
      queryAsString = json.dumps(allParams)
      try:
         response = requests.post(self.baseurl, 
                                  data=queryAsString,
                                  headers=self.headers)
      except Exception, e:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            retcode = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Unknown Error'
         print('Failed on URL {}, code = {}, reason = {} ({})'.format(
                           self.url, retcode, reason, str(e)))
      
      if response.ok:
         try:
            if outputType == 'json':
               try:
                  self.output = response.json()
               except Exception, e:
                  output = response.content
                  self.output = deserialize(fromstring(output))
#                   self.output = ET.fromstring(output)
            elif outputType == 'xml':
               output = response.text
               self.output = deserialize(fromstring(output))
#                self.output = ET.fromstring(output)
            else:
               print('Unrecognized output type {}'.format(outputType))
         except Exception, e:
            print('Failed to interpret output of URL {}, content = {}; ({})'
                  .format(self.baseurl, response.content, str(e)))
      else:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            retcode = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Unknown Error'
         print('Failed ({}: {}) for baseurl {} and query {}'
               .format(retcode, reason, self.baseurl, queryAsString))


# .............................................................................
class GbifAPI(APIQuery):
# .............................................................................
   """
   Class to query GBIF APIs and return results
   """
# ...............................................
   def __init__(self, service=GBIF.SPECIES_SERVICE, key=None, otherFilters={}):
      """
      @summary: Constructor for GbifAPI class      
      """
      url = '/'.join((GBIF.REST_URL, service))
      if key is not None:
         url = '/'.join((url, str(key)))
         APIQuery.__init__(self, url)
      else:
         APIQuery.__init__(self, url, otherFilters=otherFilters)


# ...............................................
   @staticmethod
   def _getOutputVal(outDict, name):
      try:
         tmp = outDict[name]
         val = unicode(tmp).encode('utf-8')
      except:
         return None
      return val
   
# ...............................................
   @staticmethod
   def getTaxonomy(taxonKey):
      """
      @summary: Return ITISScientificName, kingdom, and TSN hierarchy from one 
                occurrence record ending in this TSN (species rank) 
      """
      taxAPI = GbifAPI(service=GBIF.SPECIES_SERVICE, key=taxonKey)
      try:
         taxAPI.query()
         scinameStr = taxAPI._getOutputVal(taxAPI.output, 'scientificName')
         kingdomStr = taxAPI._getOutputVal(taxAPI.output, 'kingdom')
         phylumStr = taxAPI._getOutputVal(taxAPI.output, 'phylum')
         classStr = taxAPI._getOutputVal(taxAPI.output, 'class')
         orderStr = taxAPI._getOutputVal(taxAPI.output, 'order')
         familyStr = taxAPI._getOutputVal(taxAPI.output, 'family')
         genusStr = taxAPI._getOutputVal(taxAPI.output, 'genus')
         speciesStr = taxAPI._getOutputVal(taxAPI.output, 'species') 
         rankStr = taxAPI._getOutputVal(taxAPI.output, 'rank')
         genusKey = taxAPI._getOutputVal(taxAPI.output, 'genusKey')
         speciesKey = taxAPI._getOutputVal(taxAPI.output, 'speciesKey')
         acceptedKey = taxAPI._getOutputVal(taxAPI.output, 'acceptedKey')
         nubKey = taxAPI._getOutputVal(taxAPI.output, 'nubKey')
         taxStatus = taxAPI._getOutputVal(taxAPI.output, 'taxonomicStatus')
         acceptedStr = taxAPI._getOutputVal(taxAPI.output, 'accepted')
         canonicalStr = taxAPI._getOutputVal(taxAPI.output, 'canonicalName')
         loglines = []
         if taxStatus != 'ACCEPTED':
            try:
               loglines.append(taxAPI.url)
               loglines.append('   genusKey = {}'.format(genusKey))
               loglines.append('   speciesKey = {}'.format(speciesKey))
               loglines.append('   acceptedKey = {}'.format(acceptedKey))
               loglines.append('   acceptedStr = {}'.format(acceptedStr))
               loglines.append('   nubKey = {}'.format(nubKey))
               loglines.append('   taxonomicStatus = {}'.format(taxStatus))
               loglines.append('   accepted = {}'.format(acceptedStr))
               loglines.append('   canonicalName = {}'.format(canonicalStr))
               loglines.append('   rank = {}'.format(rankStr))
            except:
               loglines.append('Failed to format data from {}'.format(taxonKey))
      except Exception, e:
         print str(e)
         raise
      return (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
              nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
              familyStr, genusStr, speciesStr, genusKey, speciesKey, loglines)
 
# ...............................................
   @staticmethod
   def getPublishingOrg(puborgKey):
      """
      @summary: Return title from one organization record with this key  
      @param puborgKey: GBIF identifier for this publishing organization
      """
      orgAPI = GbifAPI(service=GBIF.ORGANIZATION_SERVICE, key=puborgKey)
      try:
         orgAPI.query()
         puborgName = orgAPI._getOutputVal(orgAPI.output, 'title')
      except Exception, e:
         print str(e)
         raise
      return puborgName
 
# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a ElementTree object 
      """
      APIQuery.queryByGet(self, outputType='json')


# .............................................................................
# .............................................................................
if __name__ == '__main__':

   
   # ******************* GBIF ********************************
   taxonid = 1000225
   output = GbifAPI.getTaxonomy(taxonid)
   print 'GBIF Taxonomy for {} = {}'.format(taxonid, output)
         
  
         
"""

"""