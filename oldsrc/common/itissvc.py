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

from riis.common import (ITIS_SOLR_URL, ITIS_NAME_KEY, ITIS_TSN_KEY,
                         ITIS_VERNACULAR_QUERY, ITIS_URL_ESCAPES,
                         ITIS_NAMESPACE, ITIS_DATA_NAMESPACE)


# .............................................................................
class ITISSvc(object):
    """Class to pull data from the ITIS Solr service, documentation at:
              https://itis.gov/solr_documentation.html
    """
# ...............................................
    def __init__(self):
        pass

# ...............................................
    def _getDataFromUrl(self, url, resp_type='json'):
        """Returns a JSON dictionary or ElementTree tree """
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
        """Return vernacular names for an ITIS TSN.

        Args:
            tsn: an ITIS code designating a taxonomic name
        """
        common_names = []
        if tsn is not None:
            url = ITIS_VERNACULAR_QUERY + str(tsn)
            root = self._getDataFromUrl(url, resp_type='xml')

            retElt = root.find('{}return'.format(ITIS_NAMESPACE))
            if retElt is not None:
                cnEltLst = retElt.findall('{}commonNames'.format(ITIS_DATA_NAMESPACE))
                for cnElt in cnEltLst:
                    nelt = cnElt.find('{}commonName'.format(ITIS_DATA_NAMESPACE))
                    if nelt is not None and nelt.text is not None:
                        common_names.append(nelt.text)
        return common_names

# ...............................................
    def get_itis_name(self, tsn):
        """Return an ITIS taxonomic name record for the first accepted name
        used for an ITIS TSN (possibly designating an unaccepted name).

        Args:
            tsn: an ITIS code designating a taxonomic name
        """
        accepted_name = kingdom = None
        url = '{}?q={}:{}&wt=json'.format(ITIS_SOLR_URL, ITIS_TSN_KEY, tsn)
        output = self._getDataFromUrl(url, resp_type='json')
        try:
            data = output['response']
        except:
            raise Exception('Failed to return response element')
        try:
            count = data['numFound']
        except:
            print('No numFound value')
        try:
            docs = data['docs']
        except:
            raise Exception('Failed to return docs')
        for doc in docs:
            usage = doc['usage']
            if usage in ('accepted', 'valid'):
                accepted_name = self._get_fld_value(doc, 'nameWOInd')
                kingdom = self._get_fld_value(doc, 'kingdom')
        return accepted_name, kingdom

# ...............................................
    def get_itis_tsn(self, sciname):
        """Return an ITIS TSN and its accepted name and kingdom for a
        scientific name.

        Args:
            sciname: a scientific name designating a taxon
        """
        tsn = accepted_name = kingdom = None
        accepted_tsn_list = []
        escname = sciname
        for replaceStr, withStr in ITIS_URL_ESCAPES:
            escname = escname.replace(replaceStr, withStr)
        url = '{}?q={}:{}&wt=json'.format(ITIS_SOLR_URL, ITIS_NAME_KEY,
                                          escname)
        output = self._getDataFromUrl(url, resp_type='json')
        try:
            data = output['response']
        except:
            raise Exception('Failed to return response element')
        try:
            count = data['numFound']
        except:
            print('No numFound value')
        try:
            docs = data['docs']
        except:
            raise Exception('Failed to return docs')
#         print('Reported/returned {}/{} docs for name {}'.format(count, len(docs), sciname))
        for doc in docs:
            usage = doc['usage']
            tsn = self._get_fld_value(doc, 'tsn')
            if usage in ('accepted', 'valid'):
                accepted_name = self._get_fld_value(doc, 'nameWOInd')
                kingdom = self._get_fld_value(doc, 'kingdom')
                break
            else:
                accepted_tsn_list = self._get_fld_value(doc, 'acceptedTSN')
#             print('Returned tsn {} acceptedTSN list {}, {} for {} name {}'
#                   .format(tsn, accepted_tsn_list, kingdom, usage, sciname))
        return tsn, accepted_name, kingdom, accepted_tsn_list
