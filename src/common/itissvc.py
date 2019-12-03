"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import requests
import xml.etree.ElementTree as ET

from common.constants import (ITIS_SOLR_URL, ITIS_NAME_KEY, ITIS_TSN_KEY, 
                              ITIS_VERNACULAR_QUERY, ITIS_URL_ESCAPES)

    
# .............................................................................
class ITISSvc(object):
    """
    @summary: Pulls data from the ITIS Solr service, documentation at:
              https://itis.gov/solr_documentation.html
    """
# ...............................................
    def __init__(self):
        pass
    
# ...............................................
    def _getDataFromUrl(self, url, resp_type='json'):
        """
        @summary: Return a JSON dictionary or ElementTree tree 
        """
        data = None
        try:
            response = requests.get(url)
        except Exception:
            print('Failed to resolve URL {}'.format(url))
        else:
            if response.status_code == 200:
                if resp_type.lower() == 'json':
                    data = response.json()
                elif resp_type.lower() == 'xml':
                    txt = response.text
                    data = ET.fromstring(txt)
                else:
                    data = response.text
        return data
                     
# ...............................................
    def _processRecordInfo(self, rec, header, reformat_keys=[]):
        row = []
        if rec is not None:
            for key in header:
                try:
                    val = rec[key]
                    
                    if type(val) is list:
                        if len(val) > 0:
                            val = val[0]
                        else:
                            val = ''
                            
                    if key in reformat_keys:
                        val = self._saveNLDelCR(val)
                        
                    elif key == 'citation':
                        if type(val) is dict:
                            try:
                                val = val['text']
                            except:
                                pass
                        
                    elif key in ('created', 'modified'):
                        val = self._clipDate(val)
                            
                except KeyError:
                    val = ''
                row.append(val)
        return row

# ...............................................
    def _get_fld_value(self, doc, fldname):
        try:
            val = doc[fldname]
        except:
            val = None
        return val

# ...............................................
    def get_itis_vernacular(self, tsn):
        data = None
        if tsn is not None:
            url = ITIS_VERNACULAR_QUERY + str(tsn)
            data = self._getDataFromUrl(url, resp_type='xml')
        else:
            raise Exception('Must provide sciname')
        
        try:
            count = data['numFound']
        except:
            raise Exception('Failed to return numFound')
        try:
            docs = data['docs']
        except:
            raise Exception('Failed to return docs')
        
        for doc in docs:
            usage = self._get_fld_value(doc, 'canonicalName')
            if usage in ('accepted', 'valid'):
                tsn = self._get_fld_value(doc, 'tsn')
            else:
                tsn = self._get_fld_value(doc, 'acceptedTSN')
        return tsn

# ...............................................
    def get_itis_tsn(self, sciname):
        data = None
        escname = sciname
        if escname is not None:
            for replaceStr, withStr in ITIS_URL_ESCAPES:
                escname = escname.replace(replaceStr, withStr)
            url = '{}?q={}:{}&wt=json'.format(ITIS_SOLR_URL, ITIS_NAME_KEY, 
                                              escname)
            data = self._getDataFromUrl(url, resp_type='json')
        else:
            raise Exception('Must provide sciname')
        
        try:
            count = data['numFound']
        except:
            print('No numFound value')
        try:
            docs = data['docs']
        except:
            raise Exception('Failed to return docs')
        
        for doc in docs:
            tsn = accepted_tsn = kingdom = None
            usage = doc['usage']
            if usage in ('accepted', 'valid'):
                tsn = self._get_fld_value(doc, 'tsn')
                kingdom = self._get_fld_value(doc, 'kingdom')
            else:
                accepted_tsn = self._get_fld_value(doc, 'acceptedTSN')
        return tsn, kingdom, accepted_tsn


"""
import requests
import xml.etree.ElementTree as ET

from common.constants import (ITIS_SOLR_URL, ITIS_NAME_KEY, ITIS_TSN_KEY, 
                              ITIS_VERNACULAR_QUERY, ITIS_URL_ESCAPES)

def _get_fld_value(doc, fldname):
    try:
        val = doc[fldname]
    except:
        val = None
    return val

sciname = 'Enteromorpha clathrata'
tsn = 173441

escname = sciname
for replaceStr, withStr in ITIS_URL_ESCAPES:
    escname = escname.replace(replaceStr, withStr)

url = '{}?q={}:{}&wt=json'.format(ITIS_SOLR_URL, ITIS_NAME_KEY, 
                                  escname)

response = requests.get(url)
response.status_code 
output = response.json()
data = output['response']
count = data['numFound']
docs = data['docs']
for doc in docs:
    tsn = accepted_tsn = kingdom = None
    usage = doc['usage']
    if usage in ('accepted', 'valid'):
        tsn = _get_fld_value(doc, 'tsn')
        kingdom = _get_fld_value(doc, 'kingdom')
    else:
        accepted_tsn = _get_fld_value(doc, 'acceptedTSN')
    print(sciname, tsn, kingdom, accepted_tsn)
    

url = ITIS_VERNACULAR_QUERY + str(tsn)
response = requests.get(url)
response.status_code 
output = response.text
data = ET.fromstring(output)

"""
