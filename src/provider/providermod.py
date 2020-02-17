import glob
import os
import time

from common.constants import (BISON_ORDERED_DATALOAD_FIELDS, BISON_VALUES,
                              BISON_PROVIDER, ProviderActions,
                              PROVIDER_DELIMITER, BISON_DELIMITER, 
                              ANCILLARY_DELIMITER, ENCODING, BISON_IPT_PREFIX)
from common.lookup import Lookup, VAL_TYPE
from common.tools import (makerow, open_csv_files)


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
    def _get_rewrite_val(self, rec, key, const_val):
        """
        @note: If both dataset-provided and record values are present, 
               record value takes precedence.
        """
        rec_val = rec[key]
        
        if not rec_val:
            return const_val
        
        if not const_val:
            return rec_val
        
        if rec_val != const_val:
            self._log.warn('{} record val {} <> const val {}'
                           .format(key, rec_val, const_val))
        return rec_val

    # ...............................................
    def _replace_resource(self, rec, resource_vals, do_rename):
        """
        @summary: Update the resource values from Jira ticket description.
                  Update the provider values with default BISON provider values.
        @note: function modifies or deletes original dict
        """            
        res_legacyid, res_name, res_url = resource_vals
        
        if rec is not None:
            if do_rename:
                rec['resource_id'] = res_legacyid
                rec['resource'] = res_name
                rec['resource_url'] = res_url
            else: 
                rec['resource_id'] = self._get_rewrite_val(rec, 'resource_id', 
                                                           res_legacyid)
                rec['resource'] = self._get_rewrite_val(rec, 'resource', res_name)
                rec['resource_url'] = self._get_rewrite_val(rec, 'resource_url', 
                                                            res_url) 
            
    # ...............................................
    def read_old_resources(self, resource_lut_fname):
        old_resources = Lookup.initFromFile(resource_lut_fname, 
                                            'legacyid',
                                            ANCILLARY_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        return old_resources
    
    # ...............................................
    def _get_resource_vals(self, tkt_resource_ident, tkt_resource_name, 
                           tkt_resource_url, old_resources=None):
        # Defaults
        res_legacyid = ''
        res_name = tkt_resource_name
        res_url = tkt_resource_url
        
        parts = tkt_resource_ident.split(',')
        if len(parts) == 2:
            if parts[0] != BISON_VALUES['provider_id']:
                raise Exception('Metadata resource identifier != {}'
                                .format(BISON_VALUES['provider_id']))
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
        return resource_vals
    
    # ...............................................
    # ...............................................
    def _fill_bison_constant_fields(self, rec):
        for key, const_val in BISON_VALUES.items():
            if key == 'provider_url':
                rval = rec[key]
                if not rval.startswith(const_val):
                    self._log.info('{} {} does not match'.format(key, rec[key]))
                    rec[key] = const_val
            else:
                rec[key] = const_val

    # ...............................................
    def _rewrite_recs(self, infile, outfile, resource_vals, action):
        do_rename = False
        if action in (ProviderActions.rename, ProviderActions.replace_rename):
            do_rename = True        
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
                self._fill_bison_constant_fields(rec)
                
                self._replace_resource(rec, resource_vals, do_rename)
                
                row = makerow(rec, BISON_ORDERED_DATALOAD_FIELDS)
                writer.writerow(row)
        except:
            raise 
        finally:
            inf.close()
            outf.close()
    
    # ...............................................
    def rewrite_bison_data(self, infile, outfile, old_resources, 
                           tkt_resource_ident, tkt_resource_name, tkt_resource_url, 
                           action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))

        if action not in (ProviderActions.wait, ProviderActions.unknown):
            old_resources = None   
            if action == ProviderActions.replace:
                old_resources = old_resources
            
            # Step 1: rewrite with updated resource/provider values
            resource_vals = self._get_resource_vals(tkt_resource_ident, 
                                                    tkt_resource_name, 
                                                    tkt_resource_url,
                                                    old_resources=old_resources)

            self._rewrite_recs(infile, outfile, resource_vals, action)
            
        else:
            print('Unknown action {} for input {}, ({})'
                  .format(action, tkt_resource_name, tkt_resource_ident))
            
          
    # ...............................................
    def assemble_files(self, inpath, old_resources):
        provider_datasets = BISON_PROVIDER.copy()
        
        prefix = os.path.join(inpath, 'bison_')
        fns = glob.glob(prefix + '440,10*.csv')
        
        if fns is not None:
            for fn in fns:
                ident = fn[len(prefix):-4]
                try:
                    found = provider_datasets[ident]
                except:
                    _, basefname = os.path.split(fn)
                    (_, res_name, res_url) = self._get_resource_vals(ident, 
                                            '', '', old_resources=old_resources)
                    if not res_name:
                        self._log.info('Dataset {} missing name'.format(ident))                        
                    elif not res_url.startswith(BISON_IPT_PREFIX):
                        self._log.info('Dataset {} has unexpected URL {}'
                                       .format(ident, res_url))
                    provider_datasets[ident] = {
                        'action': ProviderActions.rewrite, 
                        'resource': res_name,
                        'resource_url': res_url,
                        'filename': basefname}
                else:
                    self._log.info('Dataset {} will be {} by {}'.format(ident, 
                                        found['action'], found['filename']))
        return provider_datasets
