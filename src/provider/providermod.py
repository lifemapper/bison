import os
import time

from common.bisonfill import BisonFiller
from common.constants import (BISON_ORDERED_DATALOAD_FIELDS,
                              BISON_PROVIDER, ProviderActions, BISON_PROVIDER_VALUE,
                              PROVIDER_DELIMITER, BISON_DELIMITER, 
                              ANCILLARY_DELIMITER, ENCODING, LEGACY_ID_DEFAULT)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (makerow, open_csv_files, getLogger)


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
    def _replace_resource_provider(self, rec, resource_vals, provider_vals):
        """
        @summary: Update the resource values from Jira ticket description.
                  Update the provider values with default BISON provider values.
        @note: function modifies or deletes original dict
        """
        res_legacyid, res_name, res_url = resource_vals
        prv_legacyid, prv_name, prv_url = provider_vals
        if rec is not None:
            # record values
            rec_res_id = rec['resource_id']
            rec_res_name = rec['resource']
            rec_res_url = rec['resource_url']
            rec_prv_id = rec['provider_id']
            rec_prv_name = rec['provider']
            rec_prv_url = rec['provider_url']
            # Warnings: record vals <> expected vals
            if rec_res_id and rec_res_id != res_legacyid:
                self._log.warn('Record resource val {} <> ticket resource val {}'
                               .format(rec_res_id, res_legacyid)) 
            if rec_res_name and rec_res_name != res_name:
                self._log.warn('Record resource val {} <> ticket resource val {}'
                               .format(rec_res_name, res_name))
            if rec_res_url and rec_res_url != res_url:
                self._log.warn('Record resource val {} <> ticket resource val {}'
                               .format(rec_res_url, res_url))
            if rec_prv_id and rec_prv_id != prv_legacyid:
                self._log.warn('Record provider val {} <> BISON provider val {}'
                               .format(rec_prv_id, prv_legacyid)) 
            if rec_prv_name and rec_prv_name != prv_name:
                self._log.warn('Record provider val {} <> BISON provider val {}'
                               .format(rec_prv_name, prv_name))
            if rec_prv_url and rec_prv_url != prv_url:
                self._log.warn('Record provider val {} <> BISON provider val {}'
                               .format(rec_prv_url, prv_url))
            # Replace values
            rec['resource_id'] = res_legacyid
            rec['resource'] = res_name
            rec['resource_url'] = res_url
            rec['provider_id'] = prv_legacyid
            rec['provider'] = prv_name
            rec['provider_url'] = prv_url
            
            
    # ...............................................
    def read_provider_resources(self, resource_lut_fname, provider_lut_fname):
        old_resources = Lookup.initFromFile(resource_lut_fname, 
                                            'legacyid',
                                            ANCILLARY_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        old_providers = Lookup.initFromFile(provider_lut_fname, 
                                            'legacyid',
                                            ANCILLARY_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        return old_resources, old_providers
    
    # ...............................................
    def _get_resource_provider_vals(self, tkt_resource_ident, 
                                    tkt_resource_name, tkt_resource_url,
                                    old_resources=None):
        # Defaults
        res_legacyid = ''
        res_name = tkt_resource_name
        res_url = tkt_resource_url
        # DO NOT lookup in providers table 
        # Legacyid 440 returns Field Study Group of the Dutch Mammal Society
        prv_legacyid = BISON_PROVIDER_VALUE.legacyid
        prv_name = BISON_PROVIDER_VALUE.name
        prv_url = BISON_PROVIDER_VALUE.url
        
        parts = tkt_resource_ident.split(',')
        if len(parts) == 2:
            if parts[0] != BISON_PROVIDER_VALUE.legacyid:
                raise Exception('Metadata resource identifier != {}'
                                .format(BISON_PROVIDER_VALUE.legacyid))
            try:
                int(parts[1])
            except:
                pass
            else:
                res_legacyid = parts[1]
                
        if old_resources is not None and res_legacyid != '':
            try:
                metavals = old_resources.lut[res_legacyid]
                self._log.info('Found resource legacyid {} in resource table'
                               .format(res_legacyid))
            except:
                pass
            else:
                # Error if lookup succeeds and values are different
                lut_res_name = metavals['title']
                lut_res_url = metavals['homepage']
                if lut_res_name and lut_res_name != tkt_resource_name:
                    raise Exception('Lookup for resource legacyid {} returns title {} for ticket with {}'
                                    .format(res_legacyid, lut_res_name, tkt_resource_name))                
                if lut_res_url and lut_res_url != tkt_resource_url:
                    raise Exception('Lookup for resource legacyid {} returns url {} for ticket with {}'
                                    .format(res_legacyid, lut_res_url, tkt_resource_url))
        resource_vals = (res_legacyid, res_name, res_url)
        provider_vals = (prv_legacyid, prv_name, prv_url)
        return resource_vals, provider_vals
    
    # ...............................................

    
    # ...............................................
    def _rewrite_recs(self, infile, outfile, resource_vals, provider_vals):
        
        try:
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
                self._replace_resource_provider(rec, resource_vals, provider_vals)
                row = makerow(rec, BISON_ORDERED_DATALOAD_FIELDS)
                writer.writerow(row)
        except:
            raise 
        finally:
            inf.close()
    
    # ...............................................
    def rewrite_bison_data(self, infile, outfile, old_resources, old_providers,
                           tkt_resource_ident, tkt_resource_name, tkt_resource_url, 
                           action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))

        if action in (ProviderActions.add, ProviderActions.rename, 
                      ProviderActions.replace_rename, ProviderActions.replace):
            old_resources = None   
            if action == ProviderActions.replace:
                old_resources = old_resources
            
            # Step 1: rewrite with updated resource/provider values
            resource_vals, provider_vals = self._get_resource_provider_vals(
                tkt_resource_ident, tkt_resource_name, tkt_resource_url,
                old_resources=old_resources)

            self._rewrite_recs(infile, outfile, resource_vals, provider_vals)
            
        else:
            print('Unknown action {} for input {}, ({})'
                  .format(action, tkt_resource_name, tkt_resource_ident))
            
          
        
    # ...............................................
    def assemble_files(self, inpath):
        pass
