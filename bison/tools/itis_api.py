"""Module to query GBIF APIs and return data."""
import requests
import xml.etree.ElementTree as ET

from bison.common.constants import ITIS
from bison.tools.api import APISvc


# .............................................................................
class ITISSvc(APISvc):
    """Class to pull data from the ITIS Solr service.

    Documentation at: https://itis.gov/solr_documentation.html
    """
# ...............................................
    def __init__(self):
        """Constructor does nothing."""
        pass

# ...............................................
    def _get_data_from_url(self, url, resp_type='json'):
        """Get data from an API query.

        Args:
            url (str): URL for the service
            resp_type (str): type of response

        Returns:
            a JSON dictionary or ElementTree tree.
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
    def get_itis_vernacular(self, tsn):
        """Query the ITIS API and get vernacular names for an ITIS TSN.

        Args:
            tsn: an ITIS code designating a taxonomic name

        Returns:
            vernacular names (list of str) for an ITIS TSN.
        """
        common_names = []
        if tsn is not None:
            url = ITIS.VERNACULAR_QUERY + str(tsn)
            root = self._get_data_from_url(url, resp_type='xml')

            retElt = root.find('{}return'.format(ITIS.NAMESPACE))
            if retElt is not None:
                cnEltLst = retElt.findall('{}commonNames'.format(ITIS.DATA_NAMESPACE))
                for cnElt in cnEltLst:
                    nelt = cnElt.find('{}commonName'.format(ITIS.DATA_NAMESPACE))
                    if nelt is not None and nelt.text is not None:
                        common_names.append(nelt.text)
        return common_names

# ...............................................
    def get_itis_name(self, tsn):
        """Query the ITIS API and get an ITIS taxonomic name and kingdom for a TSN.

        Return the first accepted name used for a TSN (possibly designating an
        unaccepted name).

        Args:
            tsn: an ITIS code designating a taxonomic name

        Returns:
            accepted_name (str): accepted name for this TSN
            kingdom (str): kingdom for this TSN

        Raises:
            Exception: on failure to find 'response' element
            Exception: on failure to find 'docs' element

        Note:
            retrieved records without a 'usage' element are not eligible for return
        """
        accepted_name = kingdom = None
        url = "{}?q={}:{}&wt=json".format(ITIS.SOLR_URL, ITIS.TSN_KEY, tsn)
        output = self._get_data_from_url(url, resp_type='json')
        try:
            data = output["response"]
        except KeyError:
            raise Exception("Failed to find 'response' element")
        try:
            docs = data["docs"]
        except KeyError:
            raise Exception("Failed to find 'docs' element")
        # If > 1 are returned, examine until an accepted one is found, and return it
        for doc in docs:
            try:
                usage = doc["usage"]
            except KeyError:
                pass
            else:
                if usage in ("accepted", "valid"):
                    accepted_name = self._get_val(doc, "nameWOInd")
                    kingdom = self._get_val(doc, "kingdom")
                    break
        return accepted_name, kingdom

# ...............................................
    def get_itis_tsn(self, sciname):
        """Query the ITIS API and get values for a scientific name.

        Args:
            sciname: a scientific name designating a taxon

        Returns:
            tsn (int): ITIS TSN matching this valid scientific name
            accepted_name (str): accepted name for the returned TSN matching this valid scientific name
            kingdom (str): kingdom for the returned TSN matching this valid scientific name
            accepted_tsn_list (list of int): other accepted TSNs matching this *invalid* scientific name

        Raises:
            Exception: on failure to find 'response' element
            Exception: on failure to find 'docs' element

        Note:
            retrieved records without a 'usage' element are not eligible for return
        """
        tsn = accepted_name = kingdom = None
        accepted_tsn_list = []
        escname = sciname
        for replaceStr, withStr in ITIS.URL_ESCAPES:
            escname = escname.replace(replaceStr, withStr)
        url = "{}?q={}:{}&wt=json".format(ITIS.SOLR_URL, ITIS.NAME_KEY, escname)
        output = self._get_data_from_url(url, resp_type='json')
        try:
            data = output["response"]
        except KeyError:
            raise Exception("Failed to find 'response' element")
        try:
            docs = data["docs"]
        except KeyError:
            raise Exception("Failed to find 'docs' element")
#         print('Reported/returned {}/{} docs for name {}'.format(count, len(docs), sciname))
        for doc in docs:
            try:
                usage = doc["usage"]
            except KeyError:
                pass
            else:
                tsn = self._get_val(doc, 'tsn')
                if usage in ("accepted", "valid"):
                    accepted_name = self._get_val(doc, "nameWOInd")
                    kingdom = self._get_val(doc, "kingdom")
                    break
                else:
                    accepted_tsn_list = self._get_val(doc, "acceptedTSN")
    #             print('Returned tsn {} acceptedTSN list {}, {} for {} name {}'
    #                   .format(tsn, accepted_tsn_list, kingdom, usage, sciname))
        return tsn, accepted_name, kingdom, accepted_tsn_list
