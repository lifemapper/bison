import glob
import os

from common.constants import (
    BISON2020_FIELD_DEF, BISONEXPORT_TO_BISON2020_MAP, 
    BISON_PROVIDER_VALUES, PROVIDER_ACTIONS, PROVIDER_DELIMITER, BISON_DELIMITER, ENCODING, 
    BISON_IPT_PREFIX, IPT_QUERY)
from common.inputdata import BISON_PROVIDER, USDA_PLANTS_ID, USDA_PLANTS_URL
from common.lookup import Lookup, VAL_TYPE
from common.tools import (
    get_logger, makerow, open_csv_files, get_csv_dict_reader, get_csv_writer)

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
    def reset_logger2(self, logger):
        self._log = None
        self._log = logger

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
    def _replace_resource(self, rec, action, new_res_id, const_res_name, std_res_url):
        """Update the resource values from Jira ticket description.
        
        Note: 
            function modifies or deletes original dict
        """
        if rec is not None:
            # Replace all resource_ids with new value (remove legacy val)
            rec['resource_id'] = new_res_id
            # New or renamed datasets get new name and url
            if action in ('add', 'rename', 'replace_rename'):
                rec['resource'] = const_res_name
                rec['resource_url'] = std_res_url
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
                if rec_res_url != std_res_url:
                    rec['resource_url'] = std_res_url
                    # Fix, then log why mismatch
                    _, url_res_id = self._parse_bison_url(rec_res_url)
                    if url_res_id != new_res_id:
                        self._log.info('URL {} does not end with {}'
                                       .format(rec_res_url, new_res_id))
                    
    # ...............................................
    def _parse_bison_url(self, bison_url):
        standard_resource_url = new_resource_id = None
        parts = bison_url.split('=')
        if len(parts) != 2:
            self._log.info('URL {} does not parse correctly'.format(bison_url))
        else:
            urlprefix, new_resource_id = parts
            if not urlprefix.startswith(BISON_IPT_PREFIX):
                self._log.info('URL {} does not start with BISON_IPT_PREFIX'
                               .format(bison_url))
            else:
                # Standardize URL
                standard_resource_url = self._standardize_bison_url(
                    new_resource_id)
        return standard_resource_url, new_resource_id

    # ...............................................
    def _standardize_bison_url(self, bison_resource_id):
        standard_resource_url = '{}/{}{}'.format(
            BISON_IPT_PREFIX, IPT_QUERY, bison_resource_id)
        return standard_resource_url

    # ...............................................
    def read_resources(self, merged_resource_lut_fname):
        old_resources = Lookup.init_from_file(merged_resource_lut_fname, 
                                            ['BISONResourceID', 'gbif_datasetkey'], #'legacyid',
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
    def _map_old_to_new_rec(self, oldrec):
        newrec = {}
        for key in BISON2020_FIELD_DEF.keys():
            try:
                newrec[key] = oldrec[key]
            except:
                newrec[key] = None
        for oldfld, newfld in BISONEXPORT_TO_BISON2020_MAP.items():
            if newfld is not None:
                newrec[newfld] = oldrec[oldfld]
        return newrec

    # ...............................................
    def _fill_provider_constant_fields(self, rec):
        for key, const_val in BISON_PROVIDER_VALUES.items():
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
                if val.find('\"') >= 0:
                    self.loginfo('here is one!')
                rec[fld] = val.strip('\"')

    # ...............................................                
    def _remove_internal_delimiters(self, rec):
        for fld, val in rec.items():
            if isinstance(val, str):
                if val.find(BISON_DELIMITER) >= 0:
                    print ('Delimiter in val {}'.format(val))
                    rec[fld] = val.replace(BISON_DELIMITER, '')
    
    # ...............................................
    def rewrite_bison_data(self, infile, outfile, resource_key, resource_pvals, 
                           in_delimiter):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))
        
        action = resource_pvals['action']
        new_res_id = resource_pvals['resource_id']
        const_res_name = resource_pvals['resource_name']
        const_res_url = resource_pvals['resource_url']
        if not const_res_name:
            raise Exception('{} must have resource_name {}'.format(
                resource_key, new_res_id, const_res_name))
        
        if action in PROVIDER_ACTIONS:
            # Step 1: rewrite with updated resource/provider values
            self.loginfo("""{} for ticket {},
                infile {} to outfile {}
                with name {}, id {}""".format(
                    action, resource_key, infile, outfile, const_res_name, 
                    new_res_id))
            
            dl_fields = list(BISON2020_FIELD_DEF.keys())
            try:
                dict_reader, inf, csv_writer, outf = open_csv_files(
                    infile, in_delimiter, ENCODING, outfname=outfile, 
                    outfields=dl_fields, outdelimiter=BISON_DELIMITER)
                recno = 0
                for rec in dict_reader:
                    recno += 1
                    if action == 'rewrite':
                        rec = self._map_old_to_new_rec(rec)
                        
                    self._remove_internal_delimiters(rec)
                    self._fill_provider_constant_fields(rec)

                    self._replace_resource(
                        rec, action, new_res_id, const_res_name, const_res_url)
                    
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
    def fix_bison_data(self, infile, outfile, resource_key, resource_pvals):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))
        
        action = resource_pvals['action']
        new_res_id = resource_pvals['resource_id']
        const_res_name = resource_pvals['resource_name']
        const_res_url = resource_pvals['resource_url']
        if not const_res_name:
            raise Exception('{} must have resource_name {}'.format(
                resource_key, new_res_id, const_res_name))
        
        if action in PROVIDER_ACTIONS:
            # Step 1: rewrite with updated resource/provider values
            self.loginfo("""{} for ticket {},
                infile {} to outfile {}
                with name {}, id {}""".format(
                    action, resource_key, infile, outfile, const_res_name, 
                    new_res_id))
            
            dl_fields = list(BISON2020_FIELD_DEF.keys())
            try:
                # Open incomplete BISON CSV file as input
                dict_reader, inf = get_csv_dict_reader(
                    infile, BISON_DELIMITER, ENCODING)
                header = next(dict_reader)
                csv_writer, outf = get_csv_writer(outfile, BISON_DELIMITER, ENCODING)
                csv_writer.writerow(header)
                recno = 0
                for rec in dict_reader:
                    recno += 1
                    self._remove_internal_delimiters(rec)
                    
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
    def test_bison_data(self, infile, resource_key, resource_pvals):
        if not os.path.exists(infile):
            raise Exception('File {} does not exist'.format(infile))
        
        action = resource_pvals['action']
        new_res_id = resource_pvals['resource_id']
        const_res_name = resource_pvals['resource_name']
        if not const_res_name:
            raise Exception('{} must have resource_name {}'.format(
                resource_key, new_res_id, const_res_name))
        
        if action in PROVIDER_ACTIONS:
            # Step 1: rewrite with updated resource/provider values
            self.loginfo('Test ticket {}, infile {} with name {}, id {}'.format(
                action, resource_key, infile, const_res_name, new_res_id))
            
            try:
                # Open incomplete BISON CSV file as input
                dict_reader, inf = get_csv_dict_reader(
                    infile, BISON_DELIMITER, ENCODING)
                header = next(dict_reader)
                recno = 0
                probrecs = 0
                for rec in dict_reader:
                    recno += 1
                    self._remove_internal_delimiters(rec)
            except:
                raise 
            finally:
                inf.close()
                print ('Found {} problem records out of {} total'.format(
                    probrecs, recno))
        else:
            self.loginfo('Unknown action {} for input {}, ({})'.format(
                action, const_res_name, resource_key))

    # ...............................................
    def _pull_resource_from_db_or_records(self, fname, legacy_ident, 
                                          db_resource_name, db_resource_url):
        # Default - standardize db values
        new_resource_id = legacy_ident
        res_name = db_resource_name
        res_url = db_resource_url
        db_std_res_url, db_new_resource_id = self._parse_bison_url(
            db_resource_url)
        if db_std_res_url is not None:
            self.loginfo('Dataset {} has good URL in db {} '
                         .format(legacy_ident, db_resource_url))
            new_resource_id = db_new_resource_id
            res_url = db_resource_url
        else:
            self.loginfo('Dataset {} has non-standard URL in db {} '.format(
                legacy_ident, db_resource_url))

        # Check standardize record values, override db with record values
        try:
            dict_reader, inf = get_csv_dict_reader(
                fname, BISON_DELIMITER, ENCODING)
            _ = next(dict_reader)
            rec = next(dict_reader)
            for rec in dict_reader:
                rec_resource_url = rec['resource_url']
                rec_std_res_url, rec_new_resource_id = self._parse_bison_url(
                    rec_resource_url)
                if rec_std_res_url is not None:
                    self.loginfo('Dataset {} has good URL in rec {} '
                                 .format(legacy_ident, rec_resource_url))
                    new_resource_id = rec_new_resource_id
                    res_url = rec_std_res_url
                    res_name = rec['resource']
                    break
                else:
                    self.loginfo('Dataset {} has non-standard URL in rec {} '.format(
                        legacy_ident, rec_resource_url))
        except:
            raise
        finally:
            inf.close()
            
        if db_std_res_url is not None and rec_std_res_url is not None:
            if db_std_res_url != rec_std_res_url:
                self.loginfo('Dataset {} has mismatched URL in db {} and rec {}'.format(
                    legacy_ident, db_resource_url, rec_resource_url))
                
        if db_new_resource_id != rec_new_resource_id:
            self.loginfo('Dataset {} has mismatched id in db {} and rec {}'.format(
                legacy_ident, db_new_resource_id, rec_new_resource_id))
            
        return new_resource_id, res_name, res_url

    # ...............................................
    def assemble_files(self, inpath, db_resources):
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
        for key, vals in provider_datasets.items():
            vals['legacy_id'] = key
            if vals['resource_id'] == USDA_PLANTS_ID:
                vals['resource_url'] = USDA_PLANTS_URL
            else:
                vals['resource_url'] = self._standardize_bison_url(vals['resource_id'])
            provider_datasets[key] = vals
        
        prefix = os.path.join(inpath, EXISTING_DATASET_FILE_PREFIX)
        existing_prov_filenames = glob.glob(prefix + '440,10*.csv')
        
        if existing_prov_filenames is not None:
            for fn in existing_prov_filenames:
                # USDA Plants is 440, 1066
                legacy_ident = fn[len(prefix):-4]
                try:
                    newdata = provider_datasets[legacy_ident]
                except:
                    # For datasets without ticket and new data 
                    # Pull resource values from old table
                    db_resource_name, db_resource_url = \
                        self._get_old_resource_vals(legacy_ident, db_resources)                    
                    # Pull resource values old records
                    (new_resource_id, resource_name, 
                     resource_url) = self._pull_resource_from_db_or_records(
                         fn, legacy_ident, db_resource_name, db_resource_url)
                        
                    _, basefname = os.path.split(fn)
                    self.loginfo('Existing dataset {} {} will be rewritten'
                                   .format(legacy_ident, basefname))
                    
                    provider_datasets[legacy_ident] = {
                        'legacy_id': legacy_ident,
                        'action': 'rewrite', 
                        'ticket': 'Existing data',
                        'resource_name': resource_name,
                        'resource_id': new_resource_id,
                        'resource_url': resource_url,
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

        if action in PROVIDER_ACTIONS:
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
        if action in ('rename', 'rewrite'):
            delimiter = BISON_DELIMITER

        try:
            # Fill resource/provider values, use BISON_DELIMITER
            if action == 'add':
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
