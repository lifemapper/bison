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
import os
import requests

from gbif.constants import (GBIF_DSET_KEYS, GBIF_ORG_KEYS, GBIF_DSET_ORG_KEYS,
                            GBIF_UUID_KEY, GBIF_ORG_UUID_FOREIGN_KEY, GBIF_URL, 
                            GBIF_DATASET_URL, GBIF_ORGANIZATION_URL, 
                            GBIF_BATCH_PARSER_URL, GBIF_SINGLE_PARSER_URL, 
                            GBIF_TAXON_URL, 
                            OUT_DELIMITER, ENCODING, URL_ESCAPES, NEWLINE)
from gbif.tools import (getCSVWriter, getCSVDictReader, getLine, 
                        getCSVDictWriter)
from ast import parse
    
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
        return data
                     
# ...............................................
    def query_for_dataset(self, dsKey):
        dataDict = {}
        # returns GBIF dataset
        if dsKey == '':
            return dataDict
        
        url = GBIF_DATASET_URL + dsKey
        data = self._getDataFromUrl(url)
        if data is None:
            return dataDict
        
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

        return dataDict

# ...............................................
    def find_orguuid_from_dataset(self, dsKey):
        row = self.query_for_dataset(dsKey)
        try:
            publishingOrgUUID = row[0]
        except:
            print('No record for datasetKey {}'.format(dsKey))
        return publishingOrgUUID

                    
# ...............................................
    def _processRecordInfo(self, rec, header, reformat_keys=[]):
        row = []
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
    def query_write_meta(self, apitype, outfname, header, reformat_keys, 
                         UUIDs=None, delimiter=OUT_DELIMITER):
        if apitype not in ('organization', 'dataset'):
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

        if total > 0:
            try:
                outf = codecs.open(outfname, 'w', ENCODING)
                outf.write(delimiter.join(header) + NEWLINE)
                
                recno = 0
                while total <= pcount:  
                    print('Received {} {}s from GBIF'.format(len(allObjs), apitype))
                    for obj in allObjs:
                        recno += 1
                        if (UUIDs is None or obj['key'] in UUIDs):
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
                    
            except Exception:
                raise        
            finally:
                outf.close()


# ...............................................
    def get_write_organization_meta(self, outfname, delimiter=OUT_DELIMITER):
        self.query_write_meta(GBIF_ORG_KEYS.apitype, outfname, GBIF_ORG_KEYS.saveme, 
                              GBIF_ORG_KEYS.preserve_format, delimiter=delimiter)

# ...............................................
    def get_write_dataset_meta(self, outfname, uuids, delimiter=OUT_DELIMITER):
        self.query_write_meta(GBIF_DSET_KEYS.apitype, outfname, 
                              GBIF_DSET_KEYS.saveme, 
                              GBIF_DSET_KEYS.preserve_format, 
                              UUIDs=uuids, delimiter=delimiter)

    # ...............................................
    def rewrite_dataset_with_orgs(self, dsfname, outfname, delimiter=OUT_DELIMITER, 
                          encoding=ENCODING):
        '''
        @summary: Read and populate dictionary if file exists
        '''
        if not os.path.exists(dsfname):
            raise Exception('Dataset meta file {} does not exist'.format(dsfname))
        
        try:
            rdr, inf = getCSVDictReader(dsfname, delimiter, encoding)
            wrtr, outf = getCSVDictWriter(outfname, delimiter, encoding, 
                                          GBIF_DSET_ORG_KEYS.saveme)
            wrtr.writeheader()
            for dset_data in rdr:
                combined = {}
                orgUUID = dset_data[GBIF_ORG_UUID_FOREIGN_KEY]
                org_data = self.query_for_organization(orgUUID)
                
                for key in GBIF_DSET_ORG_KEYS.saveme:
                    if key.startswith('org_'):
                        okey = key[4:]
                        combined[okey] = org_data[key]
                    else:
                        combined[key] = dset_data[key]
                
                wrtr.writerow(combined)
        except Exception as e:
            self._log.error('Failed read {}, write {}, or org query {} ({})'
                            .format(dsfname, outfname, orgUUID, e))
        finally:
            inf.close()
            outf.close()

# ...............................................
    def query_for_organization(self, orgUUID):
        # returns GBIF providerId
        dataDict = {}
        if orgUUID == '':
            return dataDict
        
        url = GBIF_ORGANIZATION_URL + orgUUID
        data = self._getDataFromUrl(url)
        if data is None:
            return dataDict

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

#             row = [orgkey, legacyid, data['key'], title, desc, citation, rights, logourl,
#                      data['created'], data['modified'], homepage]
        return dataDict

# ...............................................
    def find_canonical(self, taxkey=None, sciname=None):
        canonical = None
        
        if taxkey is not None:
            url = GBIF_TAXON_URL + taxkey
            data = self._getDataFromUrl(url)

        elif sciname is not None:
            for replaceStr, withStr in URL_ESCAPES:
                sciname = sciname.replace(replaceStr, withStr)
            url = GBIF_SINGLE_PARSER_URL + sciname
            data = self._getDataFromUrl(url)
            if data is not None:
                if type(data) is list and len(data) > 0:
                    data = data[0]
        else:
            raise Exception('Must provide taxkey or sciname')
        
        if data.has_key('canonicalName'):
            canonical = data['canonicalName']
        return canonical


# ...............................................
    def _postToParser(self, url, infname):
        response = output = None
        headers={'Accept': 'application/json','Content-Type': 'application/json'}
        auth = None
        indata = open(infname, 'rb')
        
        try:
            response = requests.post(url, data=indata, headers=headers, 
                                     auth=auth)
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
        finally:
            indata.close()
            
        return output

# ...............................................
    def get_write_parsednames(self, infname, outfname, delimiter=OUT_DELIMITER,
                              overwrite=True):
#         url = GBIF_BATCH_PARSER_URL.replace('http', 'https')
        if not os.path.exists(infname):
            raise Exception('Input file {} missing'.format(outfname))
        if os.path.exists(outfname) and overwrite:
            os.remove(outfname)
        else:
            raise Exception('Output file {} exists'.format(outfname))
        
        name_fail = []
        output = self._postToParser(GBIF_BATCH_PARSER_URL, infname)
        
        if output is not None:
            try:
                csvwriter, f = getCSVWriter(outfname, delimiter)
                for rec in output:
                    try:
                        sciname = rec['scientificName']
                    except KeyError as e:
                        print('Missing scientificName in output record')
                    except Exception as e:
                        print('Failed reading scientificName in output record, err: {}'
                              .format(str(e)))
                    else:
                        if rec['parsed'] in ['true', 'True', 't', '1']:
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
                
        return name_fail
            

# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Submit data to GBIF API services as a GET request
                                     or file as a POST request.
                                 """))
    parser.add_argument('--outfile', type=str, default='/tmp/gbifapi_result_file.csv',
                        help="Full pathname for the output file")
    parser.add_argument('--name_infile', type=str, default=None,
                        help="""
                        Full pathname of the input file containing UTF-8 encoded
                        scientificNames to be parsed into canonicalNames.
                        """)
    parser.add_argument('--dataset_path', type=str, default=None,
                        help="""
                        Full pathname of the input file containing dataset UUIDs
                        for metadata retrieval.
                        """)
    args = parser.parse_args()
    outfile = args.outfile
    name_infile = args.name_infile
    dataset_path = args.dataset_path

    basepath = '/tank/data/bison/2019'
    indir = 'raw'
    tmpdir = 'tmp'
    outdir = 'out'

    outfile = os.path.join(basepath, tmpdir, 'step2.csv')
    name_infile = os.path.join(basepath, tmpdir, 'name_lookup.csv')
    dataset_infile = os.path.join(basepath, indir, 'dataset')
    
    gc = GbifAPI()
    if name_infile is not None and os.path.exists(name_infile):
        gc.get_write_parsednames(name_infile, outfile)
        
    elif dataset_path is not None and os.path.exists(dataset_path):
        import glob
        # Gather dataset UUIDs from EML files downloaded with raw data
        uuids = []
        dsfnames = glob.glob(os.path.join(dataset_path, '*.xml'))
        if dsfnames is not None:
            for fn in dsfnames:
                uuids.append(fn[:-4])
        gc.get_write_dataset_meta(outfile, uuids)
        
    else:
        gc.get_write_organization_meta(outfile)
    
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
