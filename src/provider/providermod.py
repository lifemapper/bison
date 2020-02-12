import os
import time

from common.constants import (BISON_ORDERED_DATALOAD_FIELDS,
                              BISON_PROVIDER, ProviderActions, 
                              PROVIDER_DELIMITER, BISON_DELIMITER, 
                              ANCILLARY_DELIMITER, ENCODING, LEGACY_ID_DEFAULT)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (getCSVDictReader, getCSVDictWriter, 
                          open_csv_files, getLogger)


# .............................................................................
class BisonMerger(object):
    """
    @summary: 
    """
    # ...............................................
    def __init__(self, outpath, logger):
        """
        @summary: Constructor
        """
        # Remove any trailing /
        self.outpath = outpath.rstrip(os.sep)
        self._log = logger

    # ...............................................
    def _replace_resource_vals(self, rec, old_resources, tkt_ident, 
                               tkt_resource_name, tkt_resource_url):
        tkt_resource_id = tkt_provider_id = None
        parts = tkt_ident.split(',')
        if len(parts) == 2:
            try:
                int(parts[0])
                int(parts[1])
            except:
                pass
            else:
                tkt_provider_id = parts[0]
                tkt_resource_id = parts[1]
        # record values
        rec_res_name = rec['resource']
        rec_res_id = rec['resource_id']
        rec_res_url = rec['resource_url']
        rec_prv_id = rec['provider_id']
        orgkey = None
        if rec is not None:
            try:
                metavals = old_resources.lut[tkt_resource_name]
                self._log.info('Found ticket resource name {} in resources table'
                                  .format(tkt_resource_name))
            except:
                try:
                    metavals = old_resources.lut[rec_res_name]
                    self._log.info('Found record resource name {} in resources table'
                                      .format(rec_res_name))
                except:
                    metavals = None
            if metavals is None:
                res_name = tkt_resource_name
                if tkt_resource_id is not None:
                    res_id = tkt_resource_id
                    prv_id = tkt_provider_id
                else:
                    res_id = rec_res_id
                    prv_id = rec_prv_id
                res_url = tkt_resource_url
            else:
                res_name = rec_res_name
                res_id = rec_res_id
                res_url = rec_res_url
                prv_id = rec_prv_id
                                    
            rec['resource'] = res_name
            rec['resource_id'] = res_id
            rec['resource_url'] = res_url
            rec['provider_id'] = prv_id
            
        return orgkey

    # ...............................................
    def _replace_resource_provider(self, rec, old_resources, old_providers, 
                                   resource_id, resource_name, resource_url):
        """
        @summary: Update the resource values from dataset key and metadata LUT.
                  Update the provider values from publishingOrganizationKey key 
                  in dataset metadata, and LUT from organization metadata.
                  Discard records with bison url for dataset or organization 
                  homepage.
        @note: function modifies or deletes original dict
        """
        if rec is not None:
            legacy_org_id = LEGACY_ID_DEFAULT
            title = url = ''
            orgkey = self._replace_resource_vals(rec, old_resources, resource)     
            # GBIF Organization maps to BISON provider, retrieved from dataset 
            # above + gbif organization API query and BISON provider table with legacyID
            if orgkey is not None:
                try:
                    org_metavals = orgs.lut[orgkey]
                except:
                    self._log.warning('{} missing from organization LUT'.format(orgkey))
                else:
                    title = org_metavals['title']
                    if org_metavals['homepage'] != '':
                        url = org_metavals['homepage']
                    else:
                        url = org_metavals['url']
                    legacy_org_id = org_metavals['legacyid']
                    if url.find('bison.org') >= 0:
                        self._log.info('Discard rec {}: org URL {}'
                                       .format(rec[OCC_ID_FLD], url))
                        rec = None
        if rec is not None:
            rec['provider_id'] = legacy_org_id
            rec['provider'] = title
            rec['provider_url'] = url
            
    # ...............................................
    def read_provider_resources(self, resource_lut_fname, provider_lut_fname):
        old_resources = Lookup.initFromFile(resource_lut_fname, 'title',
                                            ANCILLARY_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        old_providers = Lookup.initFromFile(provider_lut_fname, 'legacyid',
                                            ANCILLARY_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        return old_resources, old_providers
    
    # ...............................................
    def add_bison_data(self, infile, outfile, old_resources, old_providers,
                       resource_id, resource_name, resource_url):
        # Fill resource/provider values, use BISON_DELIMITER
        dict_reader, inf, writer, outf = open_csv_files(infile, 
                                         PROVIDER_DELIMITER, 
                                         ENCODING, 
                                         outfname=outfile, 
                                         outfields=BISON_ORDERED_DATALOAD_FIELDS,
                                         outdelimiter=BISON_DELIMITER)
        recno = 0
        for rec in dict_reader:
            recno += 1
            self._replace_resource_provider(rec, old_resources, old_providers, 
                                            resource_id, resource_name, 
                                            resource_url)
        
    
    # ...............................................
    def rename_bison_data(self, infile, outfile, resource_id, resource_name):
        pass
    
    # ...............................................
    def replace_bison_data(self, infile, outfile, resource_id, resource_name):
        pass
    
    # ...............................................
    def rename_replace_bison_data(self, infile, outfile, resource_id, resource_name):
        pass
    
    
    # ...............................................
    def rewrite_bison_data(self, infile, outfile, old_resources, old_providers,
                           resource_id, resource_name, resource_url, action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))
        if action == ProviderActions.add:
            self.add_bison_data(infile, outfile, old_resources, old_providers,
                                resource_id, resource_name, resource_url)
         
        elif action == ProviderActions.rename:
            self.rename_bison_data(infile, outfile, resource_id, resource_name)
      
        elif action == ProviderActions.replace:
            self.replace_bison_data(infile, outfile, resource_id, resource_name)
      
        elif action == ProviderActions.replace_rename:
            self.rename_replace_bison_data(infile, outfile, resource_id, resource_name)
 
        elif action == ProviderActions.wait:
            print('Waiting on input for {}, ({})'.format(resource_name, 
                                                         resource_id))
      
        elif action == ProviderActions.unknown:
            print('Unknown action for input {}, ({})'.format(resource_name, 
                                                             resource_id))
         
        
        print('{} action from {} to {} for {} ({})'.format(action, 
                                                           infile, outfile, 
                                                           resource_name,
                                                           resource_id))
        
    # ...............................................
    def assemble_files(self, inpath):
        pass
#         for resource_id, pvals in BISON_PROVIDER.items():
#             action = pvals['action']
#             resource = pvals['resource']
#             resource_url = pvals['resource_url']
#             fname = pvals['filename']
#             if fname is not None:
#                 infile = os.path.join(inpath, fname)
#                 if not os.path.exists(infile):
#                     raise Exception('File {} does not exist'.format(infile))
#                 basename, _ = os.path.splitext(fname)
#                 outfile = os.path.join(self.outpath, basename + '_clean.csv')
#             else:
#                 existing_fname = os.path.join(inpath, 'bison_{}.csv'
#                                               .format(resource_id))
#                 outfile = os.path.join(self.outpath, 'bison_{}_clean.csv'
#                                        .format(resource_id))
#     
#             if action == ProviderActions.add:
#                 self.rewrite_bison_data(infile, outfile, resource_id)
#             
#             elif action == ProviderActions.rename:
#                 if not os.path.exists(existing_fname):
#                     raise Exception('File {} does not exist'.format(existing_fname))
#                 self.rewrite_bison_data(existing_fname, outfile, resource_id, 
#                                         resource_name=resource)
#          
#             elif action == ProviderActions.replace:
#                 self.rewrite_bison_data(infile, outfile, resource_id)
#          
#             elif action == ProviderActions.replace_rename:
#                 self.rewrite_bison_data(infile, outfile, resource_id, 
#                                         resource_name=resource)
#     
#             elif action == ProviderActions.wait:
#                 print('Waiting on input for {}, ({})'.format(resource, 
#                                                              resource_id))
#          
#             elif action == ProviderActions.unknown:
#                 print('Unknown action for input {}, ({})'.format(resource, 
#                                                               resource_id))
         
