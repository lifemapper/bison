import glob
import os

from common.constants import (
    BISON_ORDERED_DATALOAD_FIELD_TYPE, BISON_VALUES, ProviderActions,
    PROVIDER_DELIMITER, BISON_DELIMITER, ENCODING, BISON_IPT_PREFIX)
from common.inputdata import BISON_PROVIDER
from common.lookup import Lookup, VAL_TYPE
from common.tools import (makerow, open_csv_files)

EXISTING_DATASET_FILE_PREFIX = 'bison_'
# .............................................................................
class BisonMerger(object):
    """
    @summary: 
    """
    # ...............................................
    def __init__(self, logger):
        """
        @summary: Constructor
        """
        # Remove any trailing /
        self._log = logger
        self._resource_table = {}
        self._provider_table = {}
        

    # ...............................................
    def _get_rewrite_val(self, resource_ident, rec, key, const_val):
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
            # keep shorter urls if in constant value 
            if key == 'resource_url':
                close_enough = (
                    rec_val == const_val.replace('resource', 'resource.do'))
                if not close_enough:
                    close_enough = (
                        rec_val == const_val.replace('resource', 'manage/resource'))
                    
            elif key == 'resource_id':
                # These data have incorrect val in resource_id
                if resource_ident == 'nycity-tree-census-2015':
                    return resource_ident
                # Some files contain resource_id without provider_id
                close_enough = ('440,' + rec_val == const_val)
            else:
                close_enough = False

            # Fill with constant if close
            if close_enough:
                return const_val
            
            else:
                # Save all values that occur in this dataset for name/url/ident
                # to rewrite resources table later
                try:
                    corrected_vals = self._resource_table[resource_ident]
                except:
                    self._resource_table[resource_ident] = {key: set([rec_val])}
                else:
                    try:
                        corrected_vals[key]
                    except:
                        corrected_vals[key] = set([rec_val])
                    else:
                        corrected_vals[key].add(rec_val)
                    
                return rec_val

    # ...............................................
    def _replace_resource(self, rec, resource_ident, resource_name, resource_url, 
                          action):
        """
        @summary: Update the resource values from Jira ticket description.
                  Update the provider values with default BISON provider values.
        @note: function modifies or deletes original dict
        """            
        if rec is not None:
            # Take original ticket values
            if action in (ProviderActions.rename, ProviderActions.replace_rename):
                rec['resource_id'] = resource_ident
                rec['resource'] = resource_name
                rec['resource_url'] = resource_url
            # If record val <> lookup val, saves for lookup modification
            else:
                rec['resource_id'] = self._get_rewrite_val(
                    resource_ident, rec, 'resource_id', resource_ident)
                rec['resource'] = self._get_rewrite_val(
                    resource_ident, rec, 'resource', resource_name)
                rec['resource_url'] = self._get_rewrite_val(
                    resource_ident, rec, 'resource_url', resource_url) 
            
    # ...............................................
    def read_resources(self, merged_resource_lut_fname):
        old_resources = Lookup.initFromFile(merged_resource_lut_fname, 
                                            'BISONResourceID', #'legacyid',
                                            BISON_DELIMITER, 
                                            valtype=VAL_TYPE.DICT, 
                                            encoding=ENCODING)
        return old_resources
    
    # ...............................................
    def _get_old_resource_vals(self, legacy_ident, old_resources):
        try:
            metavals = old_resources.lut[legacy_ident]
        except:
            raise Exception('No resource legacyid {} in resource table'
                            .format(legacy_ident))

        lut_res_name = metavals['name']
        lut_res_url = metavals['website_url']

        if lut_res_name is None or lut_res_url is None:
            raise Exception('No Ticket or LUT values for {}!'.format(legacy_ident))
            
        return lut_res_name, lut_res_url
    
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
    def _rewrite_recs(self, infile, outfile, resource_ident, resource_name, 
                      resource_url, action):
        delimiter = PROVIDER_DELIMITER
        # Rewrite data from existing database, with $ delimiter
        if action in (ProviderActions.rename, ProviderActions.rewrite):
            delimiter = BISON_DELIMITER

        dl_fields = list(BISON_ORDERED_DATALOAD_FIELD_TYPE.keys())
        try:
            # Fill resource/provider values, use BISON_DELIMITER
            if action == ProviderActions.add:
                pass
            dict_reader, inf, writer, outf = open_csv_files(
                infile, delimiter, ENCODING, outfname=outfile, 
                outfields=dl_fields, outdelimiter=BISON_DELIMITER)
            recno = 0
            for rec in dict_reader:
                recno += 1
                self._fill_bison_constant_fields(rec)
                
                self._replace_resource(
                    rec, resource_ident, resource_name, resource_url, action)
                
                row = makerow(rec, dl_fields)
                writer.writerow(row)
        except:
            raise 
        finally:
            inf.close()
            outf.close()
    
    # ...............................................
    def rewrite_bison_data(self, infile, outfile, resource_ident, resource_name, 
                            resource_url, action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))

        if action not in (ProviderActions.wait, ProviderActions.unknown):
            
            # Step 1: rewrite with updated resource/provider values
            self._log.info("""{} for ticket {},
                infile {} to outfile {}
                with name {}, url {}""".format(
                    action, resource_ident, infile, outfile, resource_name, 
                    resource_url))

            self._rewrite_recs(
                infile, outfile, resource_ident, resource_name, resource_url, 
                action)
            
        else:
            self._log.info('Unknown action {} for input {}, ({})'.format(
                action, resource_name, resource_ident))

    # ...............................................
    def assemble_files(self, inpath, old_resources):
        """
        Add existing provider data (resource_ident, resource_name, resource_url)
        from the BISON database to metadata and actions for new provider input 
        data from Jira tickets.  
            key BISON provider id like 440,xxxxx where 
                440 = legacy GBIF organization id and 
                xxxx = legacy GBIF dataset id, an integer
            values name, url, action, and new input filename.  
        """
        provider_datasets = BISON_PROVIDER.copy()
        
        prefix = os.path.join(inpath, EXISTING_DATASET_FILE_PREFIX)
        existing_prov_filenames = glob.glob(prefix + '440,10*.csv')
        
        if existing_prov_filenames is not None:
            for fn in existing_prov_filenames:
                legacy_ident = fn[len(prefix):-4]
                try:
                    newdata = provider_datasets[legacy_ident]
                except:
                    # Pull resource_name and resource_url from old resources
                    # database table for datasets without ticket and new data 
                    resource_name, resource_url = self._get_old_resource_vals(
                        legacy_ident, old_resources)
                    
                    if not resource_name:
                        self._log.info('Dataset {} missing name'.format(
                            legacy_ident))                        
                    elif not resource_url.startswith(BISON_IPT_PREFIX):
                        self._log.info('Dataset {} has unexpected URL {}'
                                       .format(legacy_ident, resource_url))
                        
                    _, basefname = os.path.split(fn)
                    self._log.info('Existing dataset {} {} will be rewritten'
                                   .format(legacy_ident, basefname))
                    
                    provider_datasets[legacy_ident] = {
                        'action': ProviderActions.rewrite, 
                        'ticket': 'Existing data',
                        'resource': resource_name,
                        'resource_url': resource_url,
                        'filename': basefname}
                else:
                    self._log.info('Dataset {} {} will be {} by {}'.format(
                        legacy_ident, basefname, newdata['action'], newdata['filename']))
        return provider_datasets

    # ...............................................
    def test_bison_encoding_resource(self, infile, resource_ident, 
                                     resource_name, resource_url, action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))

        if action not in (ProviderActions.wait, ProviderActions.unknown):
            # Step 1: rewrite with updated resource/provider values
            self._log.info("""
            ident {}, 
            infile {},
            name {}, 
            url {}""".format(resource_ident, infile, resource_name, resource_url))
            
            if not os.path.exists(infile):
                self._log.info(" Missing infile {}".format(infile))
            else:
                self._test_header_lines(
                    infile, resource_ident, resource_name, resource_url, action)
            
            correction_info = self.get_correction_info(
                resource_ident=resource_ident)
            self._log.info(correction_info)
            
    # ...............................................
    def get_correction_info(self, resource_ident=None):
        if resource_ident is None:             
            correction_table = '\nCorrection table\n'
            for rident, corrections in self._resource_table.items():
                correction_table += '\n{}\n'.format(rident)
                for key, val in corrections.items():
                    correction_table += '  {}:  {}\n'.format(key, str(val))
        else:
            try:
                corrections = self._resource_table[resource_ident]
            except:
                correction_table = '\nNO Corrections for {}\n'.format(resource_ident)
            else:
                correction_table = '\nCorrections for {}\n'.format(resource_ident)
                for key, val in corrections.items():
                    correction_table += '  {}:  {}\n'.format(key, str(val))
        return correction_table
            
        return correction_table

    # ...............................................
    def _test_header_lines(self, infile, resource_ident, 
                           resource_name, resource_url, action):
        import csv
        delimiter = PROVIDER_DELIMITER
        # Rewrite data from existing database, with $ delimiter
        if action in (ProviderActions.rename, ProviderActions.rewrite):
            delimiter = BISON_DELIMITER

        try:
            # Fill resource/provider values, use BISON_DELIMITER
            if action == ProviderActions.add:
                pass
            try:
                f = open(infile, 'r', encoding=ENCODING)
                header = next(f)
            except Exception as e:
                try:
                    f = open(infile, 'r', encoding='latin-1')
                    header = next(f)
                except:
                    raise Exception('Failed to read or open {} with LATIN-1 ({})'
                                    .format(infile, str(e)))
                else:
                    raise Exception('File {} is LATIN-1, should be {} ({})'
                                    .format(infile, ENCODING, str(e)))
            else:
                tmpflds = header.split(delimiter)
                fieldnames = [fld.strip() for fld in tmpflds]
                dict_reader = csv.DictReader(f, fieldnames=fieldnames, 
                                             quoting=csv.QUOTE_NONE,
                                             delimiter=delimiter)        
        
            test_count = 10
            for i in range(test_count):
                rec = next(dict_reader)
                self._replace_resource(
                    rec, resource_ident, resource_name, resource_url, action)            
        except:
            raise 
        finally:
            f.close()
    

