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
import os
import requests

from common.constants import (BISON_DELIMITER, BISON_IPT_PREFIX, 
                              ENCODING, NEWLINE, LEGACY_ID_DEFAULT) 
from common.tools import get_csv_writer

from gbif.constants import (GBIF_DSET_KEYS, GBIF_ORG_KEYS, GBIF_URL,
                            GBIF_DATASET_URL, GBIF_ORGANIZATION_URL,
                            GBIF_BATCH_PARSER_URL, GBIF_SINGLE_PARSER_URL,
                            GBIF_TAXON_URL, GBIF_URL_ESCAPES, 
                            BISON_ORG_UUID)
    
# .............................................................................
class GbifAPI(object):
    """
    @summary: Pulls UUIDs and metadata for local resolution of 
                 GBIF Organizations, Providers, Resources
    """
# ...............................................
    def __init__(self):
        pass

# ...............................................
    def _saveNL(self, strval):
        fval = strval.replace(NEWLINE, "\\n")
        return fval

# ...............................................
    def _saveNLDelCR(self, strval):
        fval = strval.replace(NEWLINE, "\\n").replace("\r", "")
        return fval

# ...............................................
    def _clipDate(self, longdate):
        dateonly = longdate.split('T')[0]
        if dateonly != '':
            parts = dateonly.split('-')
            try:
                for i in range(len(parts)):
                    int(parts[i])
            except:
                print ('Failed to parse date {} into integers'.format(longdate))
                dateonly = ''
        return dateonly
    
# ...............................................
    def _parse_date(self, datestr):
        datevals = []
        dateonly = datestr.split('T')[0]
        if dateonly != '':
            parts = dateonly.split('-')
            try:
                for i in range(len(parts)):
                    datevals.append(int(parts[i]))
            except:
                print('Invalid date {}'.format(datestr))
                pass
            else:
                if len(datevals) not in (1, 3):
                    print('Non one or three part date {}'.format(datevals))
        return datevals
    
# ...............................................
    def _first_newer(self, datevals1, datevals2):
        for i in (0,1,2):
            # only compare matching year/mo/day
            if len(datevals1) > i and len(datevals2) > i:
                if datevals1[i] > datevals2[i]:
                    return True
                elif datevals1[i] < datevals2[i]:
                    return False
        # if equal, return first_newer
        return True

# ...............................................
    def _getDataFromUrl(self, url, resp_type='json'):
        data = None
        try:
            response = requests.get(url)
        except Exception:
            print('Failed to resolve URL {}'.format(url))
        else:
            if response.status_code == 200:
                if resp_type == 'json':
                    data = response.json()
                else:
                    data = response.text
                    
            if data is not None:
                try:
                    result_count = data['count']
                except:
                    pass
                else:
                    if result_count == 0:
                        data = None
        return data

            
# ...............................................
    def _get_buried_url_val(self, data_dict, search_prefix=None):
        url = ''
        try:
            idents = data_dict['identifiers']
        except:
            pass
        else:
            for child in idents:
                if child['type'] == 'URL':
                    url = child['identifier']
                    # Return URL with desired prefix, else return last instance
                    if search_prefix and url.startswith(search_prefix):
                        break
        return url
    
# ...............................................
    def _get_val(self, data_dict, keylist, save_nl=True):
        val = ''
        child = data_dict
        for key in keylist:
            try:
                child = child[key]
            except:
                pass
#                 print('Key {} of {} missing from output'.format(key, keylist))
            else:
                val = child
        if val != '' and save_nl :
            val = self._saveNLDelCR(val)
        return val
        
# ...............................................
    def query_for_dataset(self, dsKey, is_legacyid=False):
        dataDict = {}
        if dsKey == '':
            return dataDict
        if is_legacyid:
            metaurl = '{}/{}?identifier='.format(GBIF_URL, 
                                                 GBIF_DSET_KEYS.apitype, dsKey)
        else:
            metaurl = GBIF_DATASET_URL + dsKey
        data = self._getDataFromUrl(metaurl)
        if data is None:
            return dataDict
        
        key = self._get_val(data, ['key'], save_nl=False)
        url = ''
        if key == '':
            print('Failed to resolve dataset with no key')
        else:
            orgkey = self._get_val(data, ['publishingOrganizationKey'], save_nl=True)
            if orgkey == '':
                print('Failed to find publishingOrganizationKey in {}'.format(metaurl))
            elif orgkey == BISON_ORG_UUID:
                url = self._get_buried_url_val(data, search_prefix=BISON_IPT_PREFIX)
            if url == '': 
                url = self._get_buried_url_val(data)
            if url == '': 
                url = self._get_val_or_first_of_list(data, 'homepage')
                
            legacyid = self._get_legacyid(data)

            title = self._get_val(data, ['title'], save_nl=True)
            desc = self._get_val(data, ['description'], save_nl=True)
            citation = self._get_val(data, ['citation', 'text'], save_nl=True)
            created = self._get_val(data, ['created'], save_nl=False)
            modified = self._get_val(data, ['modified'], save_nl=False)
            
            # Matches current_gbif content of MERGED_RESOURCE_LUT_FIELDS
            dataDict['gbif_datasetkey'] = key
            dataDict['gbif_legacyid'] = legacyid
            dataDict['gbif_publishingOrganizationKey'] = orgkey
            dataDict['gbif_title'] = title
            dataDict['gbif_url'] = url
            dataDict['gbif_description'] = desc
            dataDict['gbif_citation'] = citation
            dataDict['gbif_created'] = created
            dataDict['gbif_modified'] = modified
        return dataDict

# ...............................................
    def find_orguuid_from_dataset(self, dsKey):
        dataDict = self.query_for_dataset(dsKey)
        try:
            publishingOrgUUID = dataDict['publishingOrganizationKey']
        except:
            print('No record for datasetKey {}'.format(dsKey))
        return publishingOrgUUID
                    
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
    def query_write_all_meta(self, apitype, outfname, header, reformat_keys,
                             delimiter=BISON_DELIMITER):
        if apitype not in ('organization'):
            raise Exception('What kind of query is {}?'.format(apitype))

        offset = 0
        pagesize = 1000
        url = '{}/{}?offset={}&limit={}'.format(GBIF_URL, apitype, offset, pagesize)
        
        total = 0
        data = self._getDataFromUrl(url)
        if data is not None:
            pcount = data['count']
            allObjs = data['results']
            isComplete = data['endOfRecords']
            total = len(allObjs)
        if total == 0:
            print('No records returned for url {}'.format(url))
            return
        with open(outfname, 'w', encoding=ENCODING) as outf:
            outf.write(delimiter.join(header) + NEWLINE)
            
            recno = 0
            while total <= pcount:  
                print('Received {} of {} {}s from GBIF'.format(len(allObjs),
                                                               pcount, apitype))
                for obj in allObjs:
                    recno += 1
                    row = self._processRecordInfo(obj, header,
                                        reformat_keys=reformat_keys)
                    try:
                        outf.write(delimiter.join(row) + NEWLINE)
                    except Exception:
                        raise
                    
                if isComplete:
                    total = pcount + 1
                else:
                    offset += pagesize
                    url = '{}/{}?offset={}&limit={}'.format(GBIF_URL, apitype, offset, pagesize)
                    data = self._getDataFromUrl(url)
                    if data is not None:
                        allObjs = data['results']
                        isComplete = data['endOfRecords']
                        total += len(allObjs) 

# ...............................................
    def query_write_some_meta(self, apitype, outfname, header, reformat_keys,
                              UUIDs, delimiter=BISON_DELIMITER):
        if apitype not in ('species', 'dataset', 'organization'):
            raise Exception('What kind of query is {}?'.format(apitype))
        if not UUIDs:
            print('No UUIDs provided for GBIF {} query'.format(apitype))
            return
        count = 0
        loginterval = len(UUIDs) // 10
        with open(outfname, 'w', encoding=ENCODING) as outf:
            outf.write(delimiter.join(header) + NEWLINE)
            for uuid in UUIDs:
                count += 1
                url = '{}/{}/{}'.format(GBIF_URL, apitype, uuid)
                data = self._getDataFromUrl(url)
                row = self._processRecordInfo(data, header,
                                    reformat_keys=reformat_keys)
                if (count % loginterval) == 0:
                    print('*** Processed {} of {} {} queries ***'
                                   .format(count, len(UUIDs), apitype))
                try:
                    outf.write(delimiter.join(row) + NEWLINE)
                except Exception:
                    print('Failed to write row {}'.format(row))

# # ...............................................
#     def get_write_organization_meta(self, outfname, delimiter=BISON_DELIMITER):
#         self.query_write_all_meta(GBIF_ORG_KEYS.apitype, outfname,
#                                   GBIF_ORG_KEYS.saveme,
#                                   GBIF_ORG_KEYS.preserve_format,
#                                   delimiter=delimiter)

# ...............................................
    def get_write_dataset_meta(self, outfname, uuids, delimiter=BISON_DELIMITER):
        self.query_write_some_meta(GBIF_DSET_KEYS.apitype, outfname,
                                   GBIF_DSET_KEYS.saveme,
                                   GBIF_DSET_KEYS.preserve_format,
                                   uuids,
                                   delimiter=delimiter)

    # ...............................................
    def get_write_org_meta(self, outfname, uuids, delimiter=BISON_DELIMITER):
        '''
        @summary: Read and populate dictionary if file exists
        '''
        self.query_write_some_meta(GBIF_ORG_KEYS.apitype, outfname,
                                   GBIF_ORG_KEYS.saveme,
                                   GBIF_ORG_KEYS.preserve_format,
                                   uuids,
                                   delimiter=delimiter)

    # ...............................................
    def _get_val_or_first_of_list(self, data, key):
        val = ''
        if key in data:
            val = data[key]
            if isinstance(val, list) or isinstance(val, tuple):
                if len(val) > 0:
                    val = val[0]
                else:
                    val = ''
        return val
    
    # ...............................................
    def _get_legacyid(self, data):
        legacyid = LEGACY_ID_DEFAULT
        if ('identifiers' in data
             and len(data['identifiers']) > 0 
             and data['identifiers'][0]['type'] == 'GBIF_PORTAL'):
            legacyid = data['identifiers'][0]['identifier']
        return legacyid
        
    # ...............................................
    def query_for_organization(self, org_key, is_legacyid=False):
        # returns GBIF providerId
        dataDict = {}
        if org_key == '':
            return dataDict
        if is_legacyid:
            metaurl = '{}/{}?identifier='.format(GBIF_URL, 
                                                 GBIF_ORG_KEYS.apitype, org_key)
        else:
            metaurl = GBIF_ORGANIZATION_URL + org_key
        data = self._getDataFromUrl(metaurl)
        if data is None:
            return dataDict

        if 'key' in data:
            key = data['key']
            
            title = self._get_val(data, ['title'], save_nl=True)
            desc = self._get_val(data, ['description'], save_nl=True)
            citation = self._get_val(data, ['citation', 'text'], save_nl=True)
            created = self._get_val(data, ['created'], save_nl=False)
            modified = self._get_val(data, ['modified'], save_nl=False)
            
            legacyid = self._get_legacyid(data)
            url = self._get_buried_url_val(data)
            if url == '':
                url = self._get_val_or_first_of_list(data, 'homepage')

            # Matches current_gbif content of MERGED_RESOURCE_LUT_FIELDS    
            dataDict['gbif_organizationKey'] = key
            dataDict['gbif_legacyid'] = legacyid
            dataDict['gbif_title'] = title
            dataDict['gbif_url'] = url
            dataDict['gbif_description'] = desc
            dataDict['gbif_citation'] = citation
            dataDict['gbif_created'] = created
            dataDict['gbif_modified'] = modified

        return dataDict

# ...............................................
    def find_canonical(self, taxkey=None, sciname=None):
        canonical = None
        
        if taxkey is not None:
            url = GBIF_TAXON_URL + taxkey
            data = self._getDataFromUrl(url)

        elif sciname is not None:
            for replaceStr, withStr in GBIF_URL_ESCAPES:
                sciname = sciname.replace(replaceStr, withStr)
            url = GBIF_SINGLE_PARSER_URL + sciname
            data = self._getDataFromUrl(url)
            if data is not None:
                if type(data) is list and len(data) > 0:
                    data = data[0]
        else:
            raise Exception('Must provide taxkey or sciname')
        
        if 'canonicalName' in data:
            canonical = data['canonicalName']
        return canonical

# ...............................................
    def _postJsonToParser(self, url, data):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            if response is not None:
                retcode = response.status_code
            else:
                print('Failed on URL {} ({})'.format(url, str(e)))
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception as e:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    else:
                        print('Failed to interpret output of URL {} ({})'
                            .format(url, str(e)))
            else:

                try:
                    retcode = response.status_code        
                    reason = response.reason
                except:
                    print('Failed to find failure reason for URL {} ({})'
                        .format(url, str(e)))
                else:
                    print('Failed on URL {} ({}: {})'
                            .format(url, retcode, reason))
        return output

# ...............................................
    def get_write_parsednames(self, indata, outfname, delimiter=BISON_DELIMITER):
        name_fail = []
        if os.path.exists(outfname):
            fmode = 'a'
        else:
            fmode = 'w'
        csvwriter, f = get_csv_writer(outfname, delimiter, ENCODING, fmode=fmode)

        output = self._postJsonToParser(GBIF_BATCH_PARSER_URL, indata)
        total = 0
        
        if output is not None:
            total = len(output)
            try:
                for rec in output:
                    try:
                        sciname = rec['scientificName']
                    except KeyError as e:
                        print('Missing scientificName in output record')
                    except Exception as e:
                        print('Failed reading scientificName in output record, err: {}'
                              .format(str(e)))
                    else:
                        if rec['parsed'] is True:
                            try:
                                canname = rec['canonicalName']
                                csvwriter.writerow([sciname, canname])
                            except KeyError as e:
                                print('Missing canonicalName in output record')
                            except Exception as e:
                                print('Failed writing output record, err: {}'
                                      .format(str(e)))
                        else:
                            name_fail.append(sciname)
                            
            except Exception as e:
                print('Failed writing outfile {}, err: {}'.format(outfname, str(e)))                
            finally:
                f.close()
                
        return total, name_fail
            
# ...............................................
    def get_parsednames(self, indata):
        name_fail = []
        parsed_names = {}
        output = self._postJsonToParser(GBIF_BATCH_PARSER_URL, indata)
        if output is not None:
            for rec in output:
                try:
                    sciname = rec['scientificName']
                except KeyError as e:
                    print('Missing scientificName in output record')
                except Exception as e:
                    print('Failed reading scientificName in output record, err: {}'
                          .format(str(e)))
                else:
                    if rec['parsed'] is True:
                        try:
                            canname = rec['canonicalName']
                            parsed_names[sciname] = canname
                        except KeyError as e:
                            print('Missing canonicalName in output record')
                        except Exception as e:
                            print('Failed writing output record, err: {}'
                                  .format(str(e)))
                    else:
                        name_fail.append(sciname)        
        return parsed_names, name_fail
            

# ...............................................
if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser(
#                 description=("""Submit data to GBIF API services as a GET request
#                                      or file as a POST request.
#                                  """))
#     parser.add_argument('--outfile', type=str, default='/tmp/gbifapi_result_file.csv',
#                         help="Full pathname for the output file")
#     parser.add_argument('--name_infile', type=str, default=None,
#                         help="""
#                         Full pathname of the input file containing UTF-8 encoded
#                         scientificNames to be parsed into canonicalNames.
#                         """)
#     parser.add_argument('--dataset_path', type=str, default=None,
#                         help="""
#                         Full pathname of the input file containing dataset UUIDs
#                         for metadata retrieval.
#                         """)
#     args = parser.parse_args()
#     outfile = args.outfile
#     name_infile = args.name_infile
#     dataset_path = args.dataset_path

    org_uuid = '898ba450-1627-11df-bd84-b8a03c50a862'

    infname = '/tank/data/bison/2019/AS/tmp/parser_in_10.txt'
    outfname = '/tank/data/bison/2019/AS/tmp/name_parser_10.csv'
    
    gbifapi = GbifAPI()
    
#     if os.path.exists(infname):
#         names = []
#         with open(infname, 'r', encoding='utf-8') as f:
#             for line in f:
#                 names.append(line.strip())
# #         jnames = json.dumps(names)
#         gbifapi.get_write_parsednames(names, outfname)
        
    rec = gbifapi.query_for_organization(org_uuid)
        
"""

curl -i \
  --user usr:pswd \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -X POST -d @/state/partition1/data/bison/us/nameUUIDForLookup_1-10000000_sciname.json \
  http://api.gbif.org/v1/parser/name


import csv
import codecs
import cStringIO
import json
import requests

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
response = requests.get(url)
data = json.load(response)
total = data['count']
allProvs = data['results']
isComplete = data['endOfRecords']


"""
