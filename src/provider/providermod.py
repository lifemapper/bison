import glob
import os

from common.constants import (
    BISON_ORDERED_DATALOAD_FIELD_TYPE, BISON_VALUES, ProviderActions,
    PROVIDER_DELIMITER, BISON_DELIMITER, ENCODING, BISON_IPT_PREFIX)
from common.inputdata import BISON_PROVIDER
from common.lookup import Lookup, VAL_TYPE
from common.tools import (get_logger, makerow, open_csv_files, 
                          get_csv_dict_reader, get_csv_writer)

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
        self._log = logger
        self._resource_table = {}
        self._provider_table = {}
        
    # ...............................................
    def reset_logger(self, outdir, logname):
        self._log = None
        logfname = os.path.join(outdir, '{}.log'.format(logname))
        logger = get_logger(logname, logfname)
        self._log = logger

    # ...............................................
    def loginfo(self, msg):
        if self._log is None:
            print(msg)
        else:
            self._log.info(msg)
    # ...............................................
    def _get_rewrite_vals(self, rec, const_id, const_name, const_url):
        """
        @note: If both dataset-provided and record values are present, 
               record value takes precedence.
        """
        rec_res_name = rec['resource']
        if not rec_res_name:
            # Constant resource_name if missing from record
            res_name = const_name
        else:
            # Record resource_name if there
            res_name = rec_res_name
            if rec_res_name != const_name:
                self._log.info('Why does record val {} != ticket name {}??'.format(
                    rec_res_name, const_name))
            
        # resource_url should always be consistent IPT url with resource_id
        rec_res_url = rec['resource_url']
        res_url = const_url
        if rec_res_url != const_url:
            # keep shorter urls if in constant value
            parts = rec_res_url.split('=')
            if len(parts) != 2:
                self._log.info('record resource_url {} does not parse correctly. Correct val {}'
                               .format(rec_res_url, const_url))
            else:
                urlprefix, url_resource_id = parts
                if url_resource_id != const_id:
                    self._log.info('record resource_url {} does not have the correct endpoint {}'
                                   .format(rec_res_url, const_id))
                elif urlprefix != BISON_IPT_PREFIX:
                    self._log.info('record resource_url {} does not start with BISON_IPT_PREFIX. Correct val {}'
                                   .format(rec_res_url, const_url))

        return res_name, res_url

    # ...............................................
    def _replace_resource(self, rec, action, const_res_id, const_res_name):
        """Update the resource values from Jira ticket description.
        
        Note: 
            function modifies or deletes original dict
        """
        if rec is not None:
            const_res_url = '{}/resource?r={}'.format(
                BISON_IPT_PREFIX, const_res_id)
            # Replace all resource_ids with new value (remove legacy val)
            rec['resource_id'] = const_res_id
            # New or renamed datasets get new name and url
            if action in (ProviderActions.add, ProviderActions.rename, 
                          ProviderActions.replace_rename):
                rec['resource_name'] = const_res_name
                rec['resource_url'] = const_res_url
            # Replace or Rewrite datasets
            else:
                # resource_name can remain even if it doesn't match ticket
                if not rec['resource']:
                    rec['resource'] = const_res_name
                else:
                    if rec['resource'] != const_res_name:
                        self._log.info('Why does record val {} != ticket name {}??'.format(
                            rec['resource'], const_res_name))

                # resource_url should always be constant
                rec_res_url = rec['resource_url']
                if rec_res_url != const_res_url:
                    rec['resource_url'] = const_res_url
                    # Fix, then log why mismatch
                    _, url_res_id = self._parse_bison_url(rec_res_url)
                    if url_res_id != const_res_id:
                        self._log.info('URL {} does not end with {}'
                                       .format(rec_res_url, const_res_id))
                    
    # ...............................................
    def _parse_bison_url(self, bison_url):
        urlprefix = resource_id = None
        parts = bison_url.split('=')
        if len(parts) != 2:
            self._log.info('URL {} does not parse correctly'.format(bison_url))
        else:
            urlprefix, resource_id = parts
            if urlprefix != BISON_IPT_PREFIX:
                self._log.info('URL {} does not start with BISON_IPT_PREFIX'
                               .format(bison_url))
        return urlprefix, resource_id

    # ...............................................
    def read_resources(self, merged_resource_lut_fname):
        old_resources = Lookup.initFromFile(merged_resource_lut_fname, 
                                            ['BISONResourceID'], #'legacyid',
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
                    self.loginfo('{} {} does not match'.format(key, rec[key]))
                    rec[key] = const_val
            else:
                rec[key] = const_val

    # ...............................................                
    def _remove_outer_quotes(self, rec):
        for fld, val in rec.items():
            if isinstance(val, str):
                if val.index('\"'):
                    self.loginfo('here is one!')
                rec[fld] = val.strip('\"')

    # ...............................................
    def rewrite_bison_data(self, infile, outfile, resource_key, resource_pvals, 
                           in_delimiter):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))
        
        action = resource_pvals['action']
        const_res_id = resource_pvals['resource_id']
        const_res_name = resource_pvals['resource_name']
        if not const_res_name:
            raise Exception('{} must have resource_name {}'.format(
                resource_key, const_res_id, const_res_name))
        
        if action not in (ProviderActions.wait, ProviderActions.unknown):
            # Step 1: rewrite with updated resource/provider values
            self.loginfo("""{} for ticket {},
                infile {} to outfile {}
                with name {}, id {}""".format(
                    action, resource_key, infile, outfile, const_res_name, 
                    const_res_id))
            
            dl_fields = list(BISON_ORDERED_DATALOAD_FIELD_TYPE.keys())
            try:
                dict_reader, inf, csv_writer, outf = open_csv_files(
                    infile, in_delimiter, ENCODING, outfname=outfile, 
                    outfields=dl_fields, outdelimiter=BISON_DELIMITER)
                recno = 0
                for rec in dict_reader:
                    recno += 1
#                     self._remove_outer_quotes(rec)
                    self._fill_bison_constant_fields(rec)
                    # Pull the id from the URL in the first record
                    if const_res_id is None:
                        _, const_res_id = self._parse_bison_url(
                            rec['resource_url'])
                    self._replace_resource(
                        rec, action, const_res_id, const_res_name)
                    
                    row = makerow(rec, dl_fields)
                    csv_writer.writerow(row)
            except:
                raise 
            finally:
                inf.close()
                outf.close()
        else:
            self.loginfo('Unknown action {} for input {}, ({})'.format(
                action, const_res_name, resource_key))

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
                        self.loginfo('Dataset {} missing name'.format(
                            legacy_ident))                        
                    elif not resource_url.startswith(BISON_IPT_PREFIX):
                        self.loginfo('Dataset {} has unexpected URL {}'
                                       .format(legacy_ident, resource_url))
                        
                    _, basefname = os.path.split(fn)
                    self.loginfo('Existing dataset {} {} will be rewritten'
                                   .format(legacy_ident, basefname))
                    
                    provider_datasets[legacy_ident] = {
                        'action': ProviderActions.rewrite, 
                        'ticket': 'Existing data',
                        'resource_name': resource_name,
                        'resource_id': None,
                        'filename': basefname}
                else:
                    self.loginfo('Dataset {} {} will be {} by {}'.format(
                        legacy_ident, basefname, newdata['action'], newdata['filename']))
        return provider_datasets

    # ...............................................
    def test_bison_encoding_resource(self, infile, resource_ident, 
                                     resource_name, resource_url, action):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))

        if action not in (ProviderActions.wait, ProviderActions.unknown):
            # Step 1: rewrite with updated resource/provider values
            self.loginfo("""
            ident {}, 
            infile {},
            name {}, 
            url {}""".format(resource_ident, infile, resource_name, resource_url))
            
            if not os.path.exists(infile):
                self.loginfo(" Missing infile {}".format(infile))
            else:
                self._test_header_lines(
                    infile, resource_ident, resource_name, resource_url, action)
            
            correction_info = self.get_correction_info(
                resource_ident=resource_ident)
            self.loginfo(correction_info)
            
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
    

