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
import os
import time

from riis.common import (
    BISON_DELIMITER, ENCODING, LOGINTERVAL, PROHIBITED_VALS, LEGACY_ID_DEFAULT,
    EXTRA_VALS_KEY, ALLOWED_TYPE, BISON2020_FIELD_DEF, BISON_IPT_PREFIX,
    MERGED_RESOURCE_LUT_FIELDS, MERGED_PROVIDER_LUT_FIELDS)
from riis.common import Lookup, VAL_TYPE
from riis.common import (
    get_csv_reader, get_csv_dict_reader, get_csv_writer, open_csv_files,
    makerow)

from gbif.constants import (
    GBIF_DATASET_PATHS, GBIF_DELIMITER, TERM_CONVERT, META_FNAME,
    GBIF_TO_BISON2020_MAP, OCC_ID_FLD, CLIP_CHAR, BISON_ORG_UUID,
    GBIF_CONVERT_TEMP_FIELD_TYPE, GBIF_NAMEKEY_TEMP_FIELD,
    GBIF_NAMEKEY_TEMP_TYPE)
from gbif.gbifmeta import GBIFMetaReader
from gbif.gbifapi import GbifAPI

# .............................................................................
class GBIFReader(object):
    """
    @summary: GBIF Record containing CSV record of
                 * original provider data from verbatim.txt
                 * GBIF-interpreted data from occurrence.txt
    @note: To chunk the file into more easily managed small files (i.e. fewer
             GBIF API queries), split using sed command output to file like:
                sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
             where 1-5000 are lines to delete, and 10000 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, workpath, logger):
        """
        @summary: Constructor
        @param interpreted_fname: Full filename containing records from the GBIF
                 interpreted occurrence table
        @param meta_fname: Full filename containing metadata for all data files in the
                 Darwin Core GBIF Occurrence download:
                     https://www.gbif.org/occurrence/search
        @param outfname: Full filename for the output BISON CSV file
        """
        self._log = logger
        # Remove any trailing /
        workpath.rstrip(os.sep)
        self._meta_fname = os.path.join(workpath, META_FNAME)
        # Save these fields during processing to fill or compute from GBIF data
        # Individual steps may add/remove temporary fields for input/output
        self._fields = BISON2020_FIELD_DEF.copy()
        self._infields = list(BISON2020_FIELD_DEF.keys())
        # Write these fields after processing for next step
        self._outfields = list(BISON2020_FIELD_DEF.keys())
        # Map of gbif fields to save onto bison fieldnames
        self._gbif_bison_map = {}


        for gfld, bfld in GBIF_TO_BISON2020_MAP.items():
            # remove namespace designation
            gbiffld = gfld[gfld.rfind(CLIP_CHAR)+1:]
            self._gbif_bison_map[gbiffld] = bfld

        self._files = []
        # Canonical lookup tmp data, header: scientificName, taxonKeys
        self._nametaxa = {}
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0

        self._missing_orgs = set()
        self._missing_datasets = set()

        self._active_resources = {}
        self._active_providers = {}

    # ...............................................
    def _makerow(self, rec):
        row = []
        for fld in self._outfields:
            if not rec[fld]:
                row.append('')
            else:
                row.append(rec[fld])
        return row

    # ...............................................
    def _replace_resource_vals(self, rec, dataset_by_uuid):
        # GBIF Dataset <--> BISON resource, values from datasets LUT
        # LUT = BISON resource table merged with GBIF dataset API
        # Header in MERGED_RESOURCE_LUT_FIELDS
        gbif_ds_uuid = rec['resource_id']
        resource_name = resource_url = ''
        gbif_org_uuid = None
        if gbif_ds_uuid is not None:
            try:
                ds_metavals = dataset_by_uuid.lut[gbif_ds_uuid]
            except:
                self._missing_datasets.add(gbif_ds_uuid)
            else:
                # Get organization UUID from metadata
                gbif_org_uuid = ds_metavals['bison_provider_uuid']
                resource_name = ds_metavals['bison_resource_name']
                resource_url = ds_metavals['bison_resource_url']
                # Remove BISON records
                if gbif_org_uuid == BISON_ORG_UUID:
                    if (resource_url is not None
                          and resource_url.startswith(BISON_IPT_PREFIX)):
                        self._log.info('Discard rec {}: dataset URL {}'
                                       .format(rec[OCC_ID_FLD], resource_url))
                        rec = None
                else:
                    # Log other bison urls
                    if resource_url.find('bison.') >= 0:
                        self._log.info('In rec {}, found provider {} url {}'
                                       .format(rec[OCC_ID_FLD], gbif_org_uuid,
                                               resource_url))
        if rec is not None:
            rec['resource'] = resource_name
            rec['resource_url'] = resource_url

        return rec, gbif_org_uuid

    # ...............................................
    def _discard_bison_add_resource_provider(self, rec,
                                             dataset_by_uuid, org_by_uuid):
        """Update the resource values from dataset key and metadata LUT.

        Returns:
            Modified dictionary record

        Note:
            Update the provider values from publishingOrganizationKey key
            in dataset metadata, and LUT from organization metadata.
            Discard records with bison url for dataset or organization homepage.
        """
        if rec is not None:
            title = url = ''
            rec, gbif_org_uuid = self._replace_resource_vals(rec, dataset_by_uuid)
            if rec is not None:
                # Lookup UUID
                org_metavals = None
                if gbif_org_uuid is not None:
                    try:
                        org_metavals = org_by_uuid.lut[gbif_org_uuid]
                    except:
                        self._missing_orgs.add(gbif_org_uuid)
                    if org_metavals is not None:
                        # Save title and url from GBIF-returned value
                        title = org_metavals['bison_provider_name']
                        url = org_metavals['bison_provider_url']

                rec['provider_id'] = gbif_org_uuid
                rec['provider'] = title
                rec['provider_url'] = url
        return rec

    # ...............................................
    def _track_provider_resources(self, rec, providers=None, resources=None):
        prov_meta = res_meta = None
        prov_id = rec['provider_id']
        res_id = rec['resource_id']
        try:
            self._active_resources[rec['provider_id']] += 1
        except:
            self._active_resources[rec['provider_id']] = 1
        try:
            self._active_providers[rec['resource_id']] += 1
        except:
            self._active_providers[rec['resource_id']] = 1
        # Check for bad ID
        if providers is not None and resources is not None:
            try:
                prov_meta = providers.lut[prov_id]
            except:
                self._log.error('Lookup missing provider_id {}'.format(prov_id))
            try:
                res_meta = resources.lut[res_id]
            except:
                self._log.error('Lookup missing resource_id {}'.format(res_id))
        # Check for bad name, url
        if prov_meta is not None:
            if (prov_meta['bison_provider_name'] != rec['provider'] or
                prov_meta['bison_provider_url'] != rec['provider_url']):
                self._log.error('Bad provider {} vals {}, {}, should be {}, {}'
                                .format(prov_id, rec['provider'],
                                        rec['provider_url'],
                                        prov_meta['bison_provider_name'],
                                        prov_meta['bison_provider_url']))
        if res_meta is not None:
            if (res_meta['bison_resource_name'] != rec['resource'] or
                res_meta['bison_resource_url'] != rec['resource_url']):
                self._log.error('Bad resource {} vals {}, {}, should be {}, {}'
                                .format(res_id, rec['resource'],
                                        rec['resource_url'],
                                        res_meta['bison_resource_name'],
                                        res_meta['bison_resource_url']))

    # ...............................................
    def _update_point(self, brec):
        """Update the decimal longitude and latitude.  Replaces 0,0 with
        None, None and ensures that US/CA points have a negative longitude.

        Args:
            brec (:obj:`dict`): dictionary containing all fieldnames and values
                for this record

        Returns:
            Modified dictionary record

        Note:
            record must have lat/lon or countryCode but GBIF query is on
            countryCode so that will never be blank.
        """
        ctry = brec['iso_country_code']
        lat = lon = None
        try:
            lat = float(brec['latitude'])
        except:
            pass
        try:
            lon = float(brec['longitude'])
        except:
            pass

        # Change 0,0 to None
        if lat == 0 and lon == 0:
            lat = lon = None

        # Make sure US and Canada longitude is negative
        elif (ctry in ('US', 'CA') and lon and lon > 0):
            lon = lon * -1
            self._log.info('Rec {}: negated {} longitude to {}'
                           .format(brec[OCC_ID_FLD], ctry, lon))

        # Replace values in dictionary
        brec['latitude'] = lat
        brec['longitude'] = lon

    # ...............................................
    def _update_second_choices(self, brec):
        """Update some fields with one of two or more choices

        Updates verbatim_locality, with first non-blank of verbatimLocality,
        locality, habitat.  Update id, with first non-blank of occurrenceId/id,
        recordNumber/collector_number

        Args:
            brec (:obj:`dict`): dictionary containing all fieldnames and values
                for this record

        Modifies brec
            Modified dictionary record
        """
        # 1st choice
        if not brec['verbatim_locality']:
            # 2nd choice
            locality = brec['locality']
            if not locality:
                # 3rd choice
                locality = brec['habitat']
            brec['verbatim_locality'] = locality

        # Fill fields with secondary option if 1st is blank
        # id = 1) gbif occurrenceID or 2) gbif recordNumber (aka bison collector_number)
        # 1st choice
        if not brec['id']:
            # 2nd choice
            brec['id'] = brec['collector_number']

    # ...............................................
    def _update_dates(self, brec):
        """Parse eventDate into integers and update missing year value.

        Args:
            brec (:obj:`dict`): dictionary containing all fieldnames and values
                for this record

        Returns:
            Modified dictionary record

        Note:
            BISON eventDate should be ISO 8601, ex: 2018-08-01 or 2018
                GBIF combines with time (and here UTC time zone), ex: 2018-08-01T14:19:56+00:00
        """
        gid = brec[OCC_ID_FLD]
        fillyr = False
        # Test year field
        try:
            brec['year'] = int(brec['year'])
        except:
            fillyr = True

        # Test eventDate field
        tmp = brec['occurrence_date']
        if tmp is not None:
            dateonly = tmp.split('T')[0]
            if dateonly != '':
                parts = dateonly.split('-')
                try:
                    for i in range(len(parts)):
                        int(parts[i])
                except:
                    self._log.info('Rec {}: invalid occurrence_date (gbif eventDate) {}'
                                      .format(gid, brec['occurrence_date']))
                    pass
                else:
                    brec['occurrence_date'] = dateonly
                    if fillyr:
                        brec['year'] = parts[0]

    # ...............................................
    def _control_vocabulary(self, brec):
        bor = brec['basis_of_record']
        if bor in TERM_CONVERT:
            brec['basis_of_record'] = TERM_CONVERT[bor]

    # ...............................................
    def _read_estmeans_lookup(self, estmeans_fname):
        '''
        @summary: Read and populate dictionary with establishmentMeans
          (concatenated list of one or more AK, HI, L48).  Keys are TSN if
          it exists, scientificName if TSN is blank.
        @note: inputfile is tab delimited values with header:
                    scientificName    TSN    AK    HI    L48    estmeansref
        '''
        estmeans = None
        datadict = {}
        sep = ' '
        if os.path.exists(estmeans_fname):
            try:
                drdr, inf = get_csv_dict_reader(estmeans_fname, '\t', ENCODING)
                for rec in drdr:
                    tsn = rec['TSN']
                    sciname = rec['scientificName']
                    emlst = []
                    for fld in ['AK', 'HI', 'L48']:
                        if rec[fld] != '':
                            emlst.append(rec[fld])
                    emstr = sep.join(emlst)
                    if tsn != '':
                        datadict[tsn] = emstr
                    elif sciname != '':
                        datadict[sciname] = emstr
                    else:
                        self._log.error('Record {} has no TSN or name'
                                        .format(rec.values()))
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(estmeans_fname, e))
            finally:
                inf.close()
            if datadict:
                estmeans = Lookup.init_from_dict(datadict, valtype=VAL_TYPE.STRING)
        return estmeans

    # ...............................................
    def _read_name_lookup(self, name_lut_fname):
        """
        @summary: Create lookup table (type DICT) for:
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        if not os.path.exists(name_lut_fname):
            raise Exception('Input file {} missing!'.format(name_lut_fname))
        try:
            drdr, inf = get_csv_reader(name_lut_fname, BISON_DELIMITER,
                                     ENCODING)
            for name_or_key, canonical in drdr:
                self._nametaxa[name_or_key] = canonical
        except Exception as e:
            self._log.error('Failed to interpret row {} {}, {}'
                            .format(name_or_key, canonical, e))
        finally:
            inf.close()

#     # ...............................................
#     def _open_pass1_files(self, gbif_interp_fname, pass1_fname, nametaxa_fname):
#         '''
#         @summary: Read GBIF metadata, open GBIF interpreted data for reading,
#                   output file for writing
#         '''
#         # Open raw GBIF data
#         self._log.info('Open raw GBIF input file {}'.format(gbif_interp_fname))
#         rdr, inf = get_csv_reader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
#         self._files.append(inf)
#
#         # Open output BISON file
#         self._log.info('Open step1 BISON output file {}'.format(pass1_fname))
#         wtr, outf = get_csv_writer(pass1_fname, BISON_DELIMITER, ENCODING)
#         self._files.append(outf)
#         wtr.writerow(self._outfields)
#
#         # Read any existing values for lookup
#         if os.path.exists(nametaxa_fname):
#             self._log.info('Read metadata ...')
#             self._nametaxa = self._read_name_lookup(nametaxa_fname)
#
#         return rdr, wtr
#
#     # ...............................................
#     def _open_update_files(self, inbasename, outbasename):
#         '''
#         @summary: Open BISON-created CSV data for reading,
#                   new output file for writing
#         '''
#         infname = os.path.join(self.tmppath, inbasename)
#         outfname = os.path.join(self.tmppath, outbasename)
#
#         if not os.path.exists(infname):
#             raise Exception('Input file {} missing!'.format(infname))
#         # Open incomplete BISON CSV file as input
#         self._log.info('Open input file {}'.format(infname))
#         drdr, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
#         self._files.append(inf)
#
#         # Open new BISON CSV file for output, DictWriter does not order fields
#         dwtr, outf = get_csv_dict_writer(outfname, BISON_DELIMITER, ENCODING,
#                                       self._outfields)
#         dwtr.writeheader()
#         self._files.append(outf)
#         return drdr, dwtr

    # ...............................................
    def is_open(self):
        """
        @summary: Return true if any files are open.
        """
        for f in self._files:
            if not f is None and not f.closed:
                return True
        return False

    # ...............................................
    def close(self):
        '''
        @summary: Close input datafiles and output file
        '''
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        self._gbif_line = None

    # ...............................................
    def _test_for_discard(self, brec):
        """
        @summary: Remove record without name fields or with absence status
        @param brec: current record dictionary
        """
        if brec is not None:
            gid = brec[OCC_ID_FLD]
            # Required fields exist
            if (not brec['provided_scientific_name'] and not brec['taxonKey']):
                brec = None
                self._log.info('Discard brec {}: missing both sciname and taxkey'
                               .format(gid))
        if brec is not None:
            # remove records with occurrenceStatus = absence
            ostat = brec['occurrenceStatus']
            if ostat and ostat.lower() == 'absent':
                brec = None
                self._log.info('Discard brec {}: with occurrenceStatus absent'
                               .format(gid))
        return brec

    # ...............................................
    def _get_gbif_val(self, grec, gfld):
        """
        @summary: Create a list of values, ordered by BISON-requested fields in
                     ORDERED_OUT_FIELDS, with individual values and/or entire record
                     modified according to BISON needs.
        @param grec: A dictionary record of GBIF-interpreted occurrence data
        @param gfld: A GBIF fieldname for the desired value
        @return: a value for one column of a GBIF occurrence record.
        """
        val = None
        # Check column existence
        tmpval = grec[gfld]
        if tmpval is not None:
            # Test each field/value
            val = tmpval.strip()
            # Remove any characters used as delimiter in output
            if BISON_DELIMITER in val:
                val = val.replace(BISON_DELIMITER, '')
            # Replace N/A and empty string
            if val.lower() in PROHIBITED_VALS:
                val = None
        return val

    # ...............................................
    def _limit_field_content(self, gid, bfld, val):
        try:
            ftype = self._fields[bfld]['pgtype']
        except Exception as e:
            raise Exception('Field metadata is missing {} ({})'.format(bfld, e))
        else:
            try:
                flen = self._fields[bfld]['max_len']
            except:
                flen = None

            if ftype == ALLOWED_TYPE.integer:
                try:
                    int(val)
                except:
                    self._log.warn('Clear gbifid {} field {} value {} cannot be an integer'
                          .format(gid, bfld, val))
                    val = None
            elif ftype == ALLOWED_TYPE.double_precision:
                try:
                    float(val)
                except:
                    self._log.warn('Clear gbifid {} field {} value {} cannot be a float'
                          .format(gid, bfld, val))
                    val = None
            # Truncate vals too long
            elif (val is not None and flen is not None
                  and ftype == ALLOWED_TYPE.varchar and len(val) > flen):
                if val.find('clusteruri=BOLD:') < 0:
                    self._log.warn('Truncate gbifid {} field {} value {} to width {}'
                          .format(gid, bfld, val, flen))
                val = val[:flen]
        return val

    # ...............................................
    def _remove_internal_delimiters(self, rec):
        for fld, val in rec.items():
            if isinstance(val, str):
                if val.find(BISON_DELIMITER) >= 0:
                    print ('Delimiter in val {}'.format(val))
                    rec[fld] = val.replace(BISON_DELIMITER, '')

    # ...............................................
    def _gbif_to_bison(self, grec):
        """ Create a list of values, ordered by BISON-requested fields in
        ORDERED_OUT_FIELDS, with individual values and/or entire record
        modified according to BISON needs.

        Args:
            iline: A CSV record of GBIF-interpreted DarwinCore occurrence data

        Returns:
            list of ordered fields containing BISON-interpreted values for
            a single GBIF occurrence record.
        """
        gid = grec['gbifID']
        brec = {}
        try:
            extravals = grec[EXTRA_VALS_KEY]
        except Exception:
            pass
        else:
            self._log.warning("""Data misalignment? Received {} extra fields for brec {}"""
                  .format(len(extravals), gid))

        # Initialize record
        for bfld in self._infields:
            brec[bfld] = None

        # Fill values for gbif fields of interest
        for gfld, bfld in self._gbif_bison_map.items():
            val = self._get_gbif_val(grec, gfld)
            if val is not None:
                val = self._limit_field_content(gid, bfld, val)
                if isinstance(val, str):
                    if val.find(BISON_DELIMITER) >= 0:
                        val = val.replace(BISON_DELIMITER, '')
            brec[bfld] = val

        return brec

    # ...............................................
    def transform_gbif_to_bison(self, gbif_interp_fname,
                                merged_resource_lut_fname, merged_provider_lut_fname,
                                nametaxa_fname, pass1_fname):
        """Create a CSV file containing GBIF occurrence records extracted from
        the interpreted occurrence file provided from an Occurrence Download,
        in Darwin Core format.  Individual values may be calculated, modified or
        entire records discarded according to BISON requests.

        Args:
            gbif_interp_fname: input tab delimited GBIF CSV file
            merged_resource_lut_fname: file containing merged
                GBIF dataset/BISON resource data
            merged_provider_lut_fname: file containing merged
                GBIF organization/BISON provider data
            nametaxa_fname: file to be written containing scientific name and
                GBIF taxon key, used for later resolution
            pass1_fname: file to be written for CSV output of first
                transformation of GBIF records to BISON records

        Note:
            Some fields will be filled in on subsequent processing.
        """
        if self.is_open():
            self.close()
        if os.path.exists(pass1_fname):
            raise Exception('First pass output file {} exists!'.format(pass1_fname))

        gm_rdr = GBIFMetaReader(self._log)
        gbif_header = gm_rdr.get_field_list(self._meta_fname)

        # Add temporary fields, and metadata for limiting field content
        self._infields.extend(list(GBIF_CONVERT_TEMP_FIELD_TYPE.keys()))
        for fldname, fldmeta in GBIF_CONVERT_TEMP_FIELD_TYPE.items():
            self._fields[fldname] = fldmeta
        self._infields.append(GBIF_NAMEKEY_TEMP_FIELD)
        self._fields[GBIF_NAMEKEY_TEMP_FIELD] = GBIF_NAMEKEY_TEMP_TYPE
        self._outfields.append(GBIF_NAMEKEY_TEMP_FIELD)

        dataset_by_uuid = Lookup.init_from_file(merged_resource_lut_fname,
            ['gbif_datasetkey', 'dataset_id'], BISON_DELIMITER, valtype=VAL_TYPE.DICT,
            encoding=ENCODING)
        org_by_uuid = Lookup.init_from_file(merged_provider_lut_fname,
            ['gbif_organizationKey'], BISON_DELIMITER, valtype=VAL_TYPE.DICT,
            encoding=ENCODING)

        # Read any existing name/taxonkey values for lookup
        if os.path.exists(nametaxa_fname):
            self._log.info('Read name metadata ...')
            nametaxas = Lookup.init_from_file(
                nametaxa_fname, None, BISON_DELIMITER, valtype=VAL_TYPE.SET,
                encoding=ENCODING)
        else:
            nametaxas = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)

        recno = 0
        dict_reader, inf, writer, outf = open_csv_files(
            gbif_interp_fname, GBIF_DELIMITER, ENCODING,
            infields=gbif_header, outfname=pass1_fname,
            outfields=self._outfields, outdelimiter=BISON_DELIMITER)
        try:
            for orig_rec in dict_reader:
                recno += 1
                if orig_rec is None:
                    break
                elif (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                # Create new record
                brec = self._gbif_to_bison(orig_rec)
                brec = self._test_for_discard(brec)
                if brec is not None:
                    self._control_vocabulary(brec)
                    self._update_second_choices(brec)
                    # Format eventDate and fill missing year
                    self._update_dates(brec)
                    # Modify lat/lon vals if necessary
                    self._update_point(brec)
                    # Fill resource (gbif dataset) then provider (gbif organization) values
                    # Discard records with bison url for dataset or provider
                    brec = self._discard_bison_add_resource_provider(
                        brec, dataset_by_uuid, org_by_uuid)

                if brec is not None:
                    # Save scientificName / TaxonID for later lookup and replace
                    nametaxas.save_to_lookup(
                        brec['provided_scientific_name'], brec['taxonKey'])
                    # Write new ordered row
                    biline = makerow(brec, self._outfields)
                    writer.writerow(biline)
                    self._track_provider_resources(
                        brec, providers=org_by_uuid, resources=dataset_by_uuid)


        except Exception as e:
            self._log.error('Failed on line {}, e = {}'.format(recno, e))
        finally:
            inf.close()
            outf.close()

        self._log.info('Missing organization ids: {}'.format(self._missing_orgs))
        self._log.info('Missing dataset ids: {}'.format(self._missing_datasets))

        # Write all lookup values
        if len(nametaxas.lut) > 0:
            nametaxas.write_lookup(
                nametaxa_fname, ['scientificName', 'taxonKeys'], BISON_DELIMITER)

    # ...............................................
    def count_resource_providers(
            self, infname, merged_resource_lut_fname, merged_provider_lut_fname):
        """Read a CSV file of pre-processed BISON data, aggregating record
        counts for providers and resources. Optionally, check that
        resource/provider data fields are filled correctly.

        Results:
            A CSV file of provider and resource record counts
        """
        if self.is_open():
            self.close()
        if not os.path.exists(infname):
            raise Exception('Missing input file {}!'.format(infname))
        resource_lut = Lookup.init_from_file(merged_resource_lut_fname,
            ['gbif_datasetkey', 'dataset_id'], BISON_DELIMITER, valtype=VAL_TYPE.DICT,
            encoding=ENCODING)
        provider_lut = Lookup.init_from_file(merged_provider_lut_fname,
            ['gbif_organizationKey'], BISON_DELIMITER, valtype=VAL_TYPE.DICT,
            encoding=ENCODING)
        recno = 0
        dict_reader, inf = get_csv_dict_reader(infname, BISON_DELIMITER, ENCODING)
        try:
            for rec in dict_reader:
                recno += 1
                if rec is None:
                    break
                elif (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))
                # Read resource_id (provider_id,bison_legacy_resource_id)
                self._track_provider_resources(
                    rec, provider_lut=provider_lut, resource_lut=resource_lut)
        except Exception as e:
            self._log.error('Failed on line {}, e = {}'.format(recno, e))
        finally:
            inf.close()

    # ...............................................
    def write_resource_provider_stats(self, resource_count_fname,
                                      provider_count_fname, overwrite=True):
        """Write counts for each provider and resource (GBIF organization and
        dataset), identified by the UUIDs assigned by GBIF.

        Args:
            resource_count_fname: file for resource/dataset record counts
            provider_count_fname: file for provider/organization record counts
        """
        if os.path.exists(resource_count_fname) and overwrite is True:
            os.remove(resource_count_fname)
        if os.path.exists(resource_count_fname):
            self._log.info('File exists: {}'.format(resource_count_fname))
        else:
            try:
                writer, outf = get_csv_writer(
                    resource_count_fname, BISON_DELIMITER, ENCODING, fmode='w')
                writer.writerow(['dataset_uuid', 'count'])
                for uuid, count in self._active_resources.items():
                    writer.writerow([uuid, count])
            except Exception as e:
                raise Exception(
                    'Failed to write {}: {}'.format(resource_count_fname, e))
            finally:
                outf.close()

        if os.path.exists(provider_count_fname) and overwrite is True:
            os.remove(provider_count_fname)
        if os.path.exists(provider_count_fname):
            self._log.info('File exists: {}'.format(provider_count_fname))
        else:
            try:
                writer, outf = get_csv_writer(
                    provider_count_fname, BISON_DELIMITER, ENCODING, fmode='w')
                writer.writerow(['organization_uuid', 'count'])
                for uuid, count in self._active_providers.items():
                    writer.writerow([uuid, count])
            except Exception as e:
                raise Exception(
                    'Failed to write {}: {}'.format(provider_count_fname, e))
            finally:
                outf.close()


    # ...............................................
    def read_resource_provider_stats(self, resource_count_fname,
                                      provider_count_fname):
        """Write counts for each provider and resource (GBIF organization and
        dataset), identified by the UUIDs assigned by GBIF.

        Args:
            resource_count_fname: file for resource/dataset record counts
            provider_count_fname: file for provider/organization record counts
        """
        if os.path.exists(resource_count_fname):
            try:
                drdr, inf = get_csv_dict_reader(
                    resource_count_fname, BISON_DELIMITER, ENCODING)
                for rec in drdr:
                    uuid = rec['dataset_uuid']
                    ct = rec['count']
                    self._active_resources[uuid] = ct
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(resource_count_fname, e))
            finally:
                inf.close()
        if os.path.exists(provider_count_fname):
            try:
                drdr, inf = get_csv_dict_reader(
                    provider_count_fname, BISON_DELIMITER, ENCODING)
                for rec in drdr:
                    uuid = rec['organization_uuid']
                    ct = rec['count']
                    self._active_providers[uuid] = ct
            except Exception as e:
                self._log.error('Failed reading data in {}: {}'
                                .format(resource_count_fname, e))
            finally:
                inf.close()

#     # ...............................................
#     def find_gbif_record(self, gbifid):
#         """
#         @summary: Find a GBIF occurrence record identified by provided gbifID.
#         """
#         if (not self._gbif_reader or
#             not self._gbif_line):
#             raise Exception('Use open_gbif_for_search before searching')
#
#         rec = None
#         try:
#             while (not rec and self._gbif_line is not None):
#                 # Get interpreted record
#                 self._gbif_line, self._gbif_recno = getLine(self._gbif_reader,
#                                                             self._gbif_recno)
#
#                 if self._gbif_line is None:
#                     break
#                 else:
#                     if self._gbif_line[0] == gbifid:
#                         # Create new record or empty list
#                         rec = self._create_rough_bisonrec(self._gbif_line,
#                                                           self._gbif_column_map)
#                     # Where are we
#                     if (self._gbif_self.recno % LOGINTERVAL) == 0:
#                         self._log.info('*** Record number {} ***'.format(self._gbif_recno))
#             if (not rec and self._gbif_line is None):
#                 self._log.error('Failed to find {} in remaining records'.format(gbifid))
#                 self.close()
#         except Exception as e:
#             self._log.error('Failed on line {}, exception {}'.format(self._gbif_recno, e))
#         return rec
#
#     # ...............................................
#     def open_gbif_for_search(self, gbif_interp_fname):
#         """
#         @summary: Open a CSV file containing GBIF occurrence records extracted
#                      from the interpreted occurrence file provided
#                      from an Occurrence Download, in Darwin Core format.
#         """
#         if self.is_open():
#             self.close()
#         # Open raw GBIF data
#         self._gbif_reader, inf = get_csv_reader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
#         self._files.append(inf)
#         # Pull the header row
#         self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 0)


    # ...............................................
    def gather_name_input(self, pass1_fname, nametaxa_fname):
        """Gather list of scientific names, with associated taxon keys to use
        for input to GBIF parser to output accepted canonical names.

        Args:
            pass1_fname,
            nametaxa_fname

        Note:
            Only used if the first processing step (transform_gbif_to_bison)
            fails to collect names and taxon keys into a file.
        """
        recno = 0
        try:
            self._log.info('Open initial pre-processed BISON output file {}'
                           .format(pass1_fname))
            # Open output BISON file
            dreader, inf = get_csv_dict_reader(pass1_fname, BISON_DELIMITER,
                                            ENCODING)
            # CSV of name and one or more taxon keys
            nametaxa_lut = Lookup(valtype=VAL_TYPE.SET, encoding=ENCODING)
            for rec in dreader:
                recno += 1
                nametaxa_lut.save_to_lookup(rec['provided_scientific_name'],
                                            rec['taxonKey'])
                # Show progress
                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))

        except Exception as e:
            self._log.error('Failed on line {}, exception {}'.format(recno, e))
        finally:
            inf.close()

        # Write all lookup values
        nametaxa_lut.write_lookup(
            nametaxa_fname, ['scientificName', 'taxonKeys'], BISON_DELIMITER)

    # ...............................................
    def update_bison_names(self, infname, outfname, names, track_providers=False):
        """Create a CSV file from pre-processed GBIF data, filling the field
        clean_provided_scientific_name with output from API resolution
        of original scientificName or taxonKey.

        Args:
            infname: input CSV file of data
            outfname: output CSV file of data with the field
                clean_provided_scientific_name filled
            names: Lookup object containing table of names for replacement
                verbatim_name: clean_provided_scientific_name
                taxon_key: clean_provided_scientific_name
            track_providers: boolean indicating whether to track record
                counts for resources and providers
        """
        self._infields.append(GBIF_NAMEKEY_TEMP_FIELD)
        recno = 0
        try:
            dict_reader, inf, writer, outf = open_csv_files(
                infname, BISON_DELIMITER, ENCODING, outfname=outfname,
                outfields=self._outfields)
            for rec in dict_reader:
                recno += 1
                clean_name = None
                gid = rec[OCC_ID_FLD]
                # Update record
                verbatimname = rec['provided_scientific_name']
                # Update record
                try:
                    clean_name = names.lut[verbatimname]
                except Exception as e:
                    taxkey = rec[GBIF_NAMEKEY_TEMP_FIELD]
                    try:
                        clean_name = names.lut[taxkey]
                    except Exception as e:
                        # Do not log all BOLD entries, ugh
                        if not (verbatimname.startswith('BOLD')
                                or verbatimname == 'incertae sedis'):
                            self._log.info('Rec {}: Discard rec w/ unresolved {}/{} in LUT'
                                           .format(gid, verbatimname, taxkey))
                if clean_name is not None:
                    rec['clean_provided_scientific_name'] = clean_name
                    row = self._makerow(rec)
                    writer.writerow(row)
                    if track_providers:
                        self._track_provider_resources(
                            rec)

                if (recno % LOGINTERVAL) == 0:
                    self._log.info('*** Record number {} ***'.format(recno))

        except Exception as e:
            self._log.error('Failed reading data from line {} in {}: {}'
                            .format(recno, infname, e))
        finally:
            inf.close()
            outf.close()

    # ...............................................
    def _get_dataset_uuids(self, dataset_path):
        """
        @summary: Get dataset UUIDs from downloaded dataset EML filenames.
        @param dataset_pth: absolute path to the dataset EML files
        """
        import glob
        uuids = set()
        dsfnames = glob.glob(os.path.join(dataset_path, '*.xml'))
        if dsfnames is not None:
            start = len(dataset_path)
            if not dataset_path.endswith(os.pathsep):
                start += 1
            stop = len('.xml')
            for fn in dsfnames:
                uuids.add(fn[start:-stop])
        self._log.info('Read {} dataset UUIDs from filenames in {}'
                       .format(len(uuids), dataset_path))
        return uuids

    # ...............................................
    def _write_merged_provider_lookup(self, org_uuids, gbifapi, old_providers,
                                 merged_provider_lut_fname, outdelimiter):
        """Write a lookup table for BISON provider (GBIF organization).

        The lookup merges the old BISON provider db table, with current GBIF
        metadata values for the GBIF organization UUID.

        Args:
            org_uuids: list of GBIF organization UUIDs from dataset records
            gbifapi: gbif.gbifapi.GbifAPI to query GBIF web services
            old_providers: common.lookup.Lookup to manage reading/writing lookup
                for old providers db table
            merged_provider_lut_fname: output file for the merged table
            outdelimiter: field value separator for the output file
        """
        resolved_uuids = set()
        merged_lut = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        header = [fld for (fld, _) in MERGED_PROVIDER_LUT_FIELDS]

        # First, get metadata for record in old table
        for legacyid, oldrec in old_providers.lut.items():
            merged_rec = {}
            for key, val in oldrec.items():
                merged_rec[key] = val
            # These fields are to be used in record population.
            # Start with existing values
            merged_rec['bison_provider_uuid'] = oldrec['organization_id']
            merged_rec['bison_provider_legacy_id'] = oldrec['BISONProviderID']
            merged_rec['bison_provider_name'] = oldrec['name']
            merged_rec['bison_provider_url'] = oldrec['website_url']
            uuid = oldrec['organization_id']
            newrec = None
            # query for current metadata from GBIF
            if uuid is not None and len(uuid) > 20:
                newrec = gbifapi.query_for_organization(uuid)
                if newrec:
                    resolved_uuids.add(uuid)
            if not newrec:
                newrec = gbifapi.query_for_organization(
                    legacyid, is_legacyid=True)
            if newrec:
                self._log.info('Found org legacyid {}'.format(legacyid))
                # Add newrec keys and their values to merged_rec
                for key, val in newrec.items():
                    merged_rec[key] = val
                # Overwrite some record population vals with latest metadata
                merged_rec['bison_provider_uuid'] = newrec['gbif_organizationKey']
                merged_rec['bison_provider_legacy_id'] = newrec['gbif_legacyid']
                merged_rec['bison_provider_name'] = newrec['gbif_title']
                merged_rec['bison_provider_url'] = newrec['gbif_url']
            else:
                self._log.info('No current metadata for organization legacyid {}'
                               .format(legacyid))
            # Save all keys/vals to merged LUT
            merged_lut.save_to_lookup(legacyid, merged_rec)

        # Next, get current metadata for any UUIDs not already resolved
        unresolved_uuids = org_uuids.difference(resolved_uuids)
        for uuid in unresolved_uuids:
            newrec = gbifapi.query_for_organization(uuid)
            if newrec:
                merged_rec = {}
                # Add all newrec keys and their values to merged_rec
                for key, val in newrec.items():
                    merged_rec[key] = val
                # Overwrite some record population vals with latest metadata
                merged_rec['bison_provider_uuid'] = newrec['gbif_organizationKey']
                merged_rec['bison_provider_legacy_id'] = newrec['gbif_legacyid']
                merged_rec['bison_provider_name'] = newrec['gbif_title']
                merged_rec['bison_provider_url'] = newrec['gbif_url']
                # Save new keys/vals to merged LUT
                if newrec['gbif_legacyid'] != LEGACY_ID_DEFAULT:
                    merged_lut.save_to_lookup(newrec['gbif_legacyid'], merged_rec)
                else:
                    merged_lut.save_to_lookup(uuid, merged_rec)

        merged_lut.write_lookup(merged_provider_lut_fname, header, outdelimiter)
        self._log.info('Wrote organization metadata to {}'.format(
            merged_provider_lut_fname))

    # ...............................................
    def _write_merged_resource_lookup(self, gbifapi, old_resources,
                                     merged_resource_lut_fname, outdelimiter):
        """Write a lookup table for BISON resources (GBIF dataset).

        The lookup merges the old BISON resource db table, with current GBIF
        metadata values for the GBIF dataset UUID.

        Args:
            gbifapi: gbif.gbifapi.GbifAPI to query GBIF web services
            old_resources: common.lookup.Lookup to manage reading/writing lookup
                for old resources db table
            merged_resource_lut_fname: output file for the merged table
            outdelimiter: field value separator for the output file
        """
        all_ds_uuids = set()
        for dpth in GBIF_DATASET_PATHS:
            curr_ds_uuids = self._get_dataset_uuids(dpth)
            all_ds_uuids = all_ds_uuids.union(curr_ds_uuids)

        resolved_uuids = set()
        merged_lut = Lookup(valtype=VAL_TYPE.DICT, encoding=ENCODING)
        header = [fld for (fld, _) in MERGED_RESOURCE_LUT_FIELDS]

        # First, get values for record in old table
        for legacyid, oldrec in old_resources.lut.items():
            merged_rec = {}
            for key, val in oldrec.items():
                merged_rec[key] = val
            # These fields are to be used in record population.
            # Start with existing values
            merged_rec['bison_provider_uuid'] = oldrec['owningorganization_id']
            merged_rec['bison_provider_legacy_id'] = oldrec['BISONProviderID']
            merged_rec['bison_provider_name'] = oldrec['provider_name']
            merged_rec['bison_provider_url'] = oldrec['provider_url']
            uuid = oldrec['dataset_id']
            merged_rec['bison_resource_uuid'] = uuid
            merged_rec['bison_resource_legacy_id'] = oldrec['OriginalResourceID']
            merged_rec['bison_resource_name'] = oldrec['name']
            merged_rec['bison_resource_url'] = oldrec['website_url']
            # Now check for new metadata from GBIF
            newrec = None
            if uuid is not None and len(uuid) > 20:
                newrec = gbifapi.query_for_dataset(uuid)
                if newrec:
                    resolved_uuids.add(uuid)
            # If no UUID or unresolved UUID, try with legacy id
            if not newrec:
                newrec = gbifapi.query_for_dataset(legacyid, is_legacyid=True)
            if newrec:
                self._log.info('Found dataset legacyid {}'.format(legacyid))
                # Add all newrec keys and their values to merged_rec
                for key, val in newrec.items():
                    merged_rec[key] = val
                # Overwrite some record population vals with latest metadata
                merged_rec['bison_provider_uuid'] = newrec['gbif_publishingOrganizationKey']
                merged_rec['bison_resource_uuid'] = newrec['gbif_datasetkey']
                merged_rec['bison_resource_name'] = newrec['gbif_title']
                merged_rec['bison_resource_url'] = newrec['gbif_url']

            else:
                self._log.info('No current metadata for dataset legacyid {}'
                               .format(legacyid))
            # Save all keys/vals to merged LUT
            merged_lut.save_to_lookup(legacyid, merged_rec)

        # Next, get metadata for any UUIDs not in old table
        unresolved_uuids = all_ds_uuids.difference(resolved_uuids)
        for uuid in unresolved_uuids:
            newrec = gbifapi.query_for_dataset(uuid)
            if newrec:
                merged_rec = {}
                # Add all newrec keys and their values to merged_rec
                for key, val in newrec.items():
                    merged_rec[key] = val
                # Overwrite some record population vals with latest metadata
                merged_rec['bison_provider_uuid'] = newrec['gbif_publishingOrganizationKey']
                merged_rec['bison_resource_uuid'] = newrec['gbif_datasetkey']
                merged_rec['bison_resource_name'] = newrec['gbif_title']
                merged_rec['bison_resource_url'] = newrec['gbif_url']
                # Save merged keys/vals to merged LUT
                if newrec['gbif_legacyid'] != LEGACY_ID_DEFAULT:
                    merged_lut.save_to_lookup(newrec['gbif_legacyid'], merged_rec)
                else:
                    merged_lut.save_to_lookup(uuid, merged_rec)

        merged_lut.write_lookup(merged_resource_lut_fname, header, outdelimiter)
        # Query/save dataset information
        self._log.info('Wrote dataset metadata to {}'.format(
            merged_resource_lut_fname))
        return merged_lut

    # ...............................................
    def resolve_provider_resource_for_lookup(
            self, orig_resource_fname, orig_provider_fname,
            merged_gbif_resource_fname, merged_gbif_provider_fname,
            outdelimiter=BISON_DELIMITER):
        """Write merged BISON resource and provider lookup tables to files.

        Each lookup table will contain
            * fields (with original field names) for legacy table information
              from the existing BISON database,
            * fields (prefixed by 'gbif_') for updated information from
              GBIF APIs for the GBIF UUID referenced, and
            * fields (prefixed by 'bison_' to be used for populating provider
              and resource fields in BISON record.

        Args:
            orig_resource_fname: input file containing existing BISON resource
                db table.
            orig_provider_fname: input file containing existing BISON provider
                db table.
            merged_gbif_resource_fname: output file for the merged dataset table
            merged_gbif_provider_fname: output file for the merged organization table
            outdelimiter: field value separator for the output files

        Results:
            Two files, one containing merged dataset metadata, and one
            containing merged provider metadata.

        Note:
            The 'bison_' prefixed fields ensure that the logic for filling
            fields is contained within table creation, not within record
            population.
        """
        gbifapi = GbifAPI()
        if os.path.exists(merged_gbif_resource_fname):
            self._log.info('Merged output file {} exists!'.format(
                merged_gbif_resource_fname))
        else:
            old_resources = Lookup.init_from_file(
                orig_resource_fname, ['OriginalResourceID'], BISON_DELIMITER,
                valtype=VAL_TYPE.DICT, encoding=ENCODING, ignore_quotes=False)
            merged_datasets = self._write_merged_resource_lookup(
                gbifapi, old_resources, merged_gbif_resource_fname, outdelimiter)

        if os.path.exists(merged_gbif_provider_fname):
            self._log.info('Output file {} exists!'.format(merged_gbif_provider_fname))
        else:
            old_providers = Lookup.init_from_file(
                orig_provider_fname, ['OriginalProviderID'], BISON_DELIMITER,
                valtype=VAL_TYPE.DICT, encoding=ENCODING, ignore_quotes=False)

            # --------------------------------------
            # Gather organization UUIDs from dataset metadata assembled (LUT or file)
            org_uuids = set()
            try:
                for key, ddict in merged_datasets.lut.items():
                    provider_uuid = ddict['bison_provider_uuid']
                    if provider_uuid not in (None, ''):
                        org_uuids.add(ddict['bison_provider_uuid'])
                    else:
                        print('No bison_provider_uuid in dataset {}'.format(key))
            except Exception:
                gmetardr = GBIFMetaReader(self._log)
                org_uuids = gmetardr.get_organization_uuids(
                    merged_gbif_resource_fname)

            self._write_merged_provider_lookup(
                org_uuids, gbifapi, old_providers, merged_gbif_provider_fname,
                outdelimiter)


    # ...............................................
    def _append_resolved_taxkeys(self, lut, lut_fname, name_fails, nametaxa,
                                 delimiter=BISON_DELIMITER):
        """
        @summary: Create lookup table for:
                  BISON canonicalName from GBIF scientificName and/or taxonKey
        """
        csvwriter, f = get_csv_writer(lut_fname, delimiter, ENCODING, fmode='a')
        count = 0
        names_resolved = []
        gbifapi = GbifAPI()
        try:
            for badname in name_fails:
                taxonkeys = nametaxa[badname]
                for tk in taxonkeys:
                    canonical = gbifapi.find_canonical(taxkey=tk)
                    if canonical is not None:
                        count += 1
                        lut[tk] = canonical
                        csvwriter.writerow([tk, canonical])
                        self._log.info('Appended {} taxonKey/clean_provided_scientific_name to {}'
                                       .format(count, lut_fname))
                        names_resolved.append(badname)
                        break
        except Exception:
            pass
        finally:
            f.close()

        # Log failed names for possible problems
        for name in names_resolved:
            name_fails.remove(name)
        loglist = {'BOLD': 0}
        for name in name_fails:
            if name.startswith('BOLD'):
                loglist['BOLD'] += 1
            else:
                try:
                    loglist[name] += 1
                except:
                    loglist[name] = 1

        self._log.info("""Wrote {} taxkey/canonical pairs to {} ({} unresolvable)
        Unresolved:
        {}"""
        .format(len(names_resolved), lut_fname, len(name_fails),
                ['{}: {}'.format(name, cnt) for name, cnt in loglist.items()]))


    # ...............................................
    def _write_parsed_names(self, lut_fname, namelst, delimiter=BISON_DELIMITER):
        tot = 1000
        name_dict = {}
        name_fails = []
        try:
            csvwriter, f = get_csv_writer(lut_fname, delimiter, ENCODING, fmode='w')
            header = ['provided_scientific_name_or_taxon_key', 'clean_provided_scientific_name']
            csvwriter.writerow(header)
            gbifapi = GbifAPI()
            while namelst:
                # Write first 1000, then delete first 1000
                currnames = namelst[:tot]
                namelst = namelst[tot:]
                # Get, write parsed names
                parsed_names, currfail = gbifapi.get_parsednames(currnames)
                # If > 10% fail, test for BOLD or pause
                fail_rate = len(currfail) / tot
                if fail_rate > 0.1:
                    non_sn_count = 0
                    for sn in currfail:
                        if sn.find(':') >= 0:
                            non_sn_count += 1
                    non_sn_rate = non_sn_count / len(currfail)
                    if non_sn_rate < 0.8:
                        time.sleep(10)
                        parsed_names, currfail = gbifapi.get_parsednames(currnames)
                name_fails.extend(currfail)
                for sciname, canonical in parsed_names.items():
                    name_dict[sciname] = canonical
                    csvwriter.writerow([sciname, canonical])
                self._log.info('Wrote {} sciname/canonical pairs ({} failed) to {}'
                               .format(len(parsed_names), len(currfail), lut_fname))
        except Exception as e:
            self._log.error('Failed writing parsed names {}'.format(e))
        finally:
            f.close()
        return name_dict, name_fails


    # ...............................................
    def resolve_canonical_taxonkeys_for_lookup(self, nametaxa_fname, name_lut_fname,
                                  delimiter=BISON_DELIMITER):
        """ Create lookup table for scientific names and GBIF taxonKeys,
        matching them to a clean_provided_scientific_name

        Args:
            nametaxa_fname: input CSV file of scientific names and taxon keys
            name_lut_fname: output CSV file for lookup table created to match
                scientific names and taxon keys to clean_provided_scientific_name
            delimiter: delimiter for input and output CSV files
        """
        if not os.path.exists(nametaxa_fname):
            raise Exception('Input file {} missing!'.format(nametaxa_fname))

        if os.path.exists(name_lut_fname):
            # Read existing lookup
            self._log.info('Output LUT file {} exists'.format(name_lut_fname))
            self._read_name_lookup(name_lut_fname)
            canonical_lut = Lookup.init_from_dict(self._nametaxa,
                                                valtype=VAL_TYPE.STRING,
                                                encoding=ENCODING)
        else:
            # Read name/taxonIDs dictionary for name resolution
            nametaxa = Lookup.init_from_file(nametaxa_fname, ['scientificName'],
                                           delimiter, valtype=VAL_TYPE.SET)
            # Create name LUT with messyname/canonical from GBIF parser and save to file
            name_dict, name_fails = self._write_parsed_names(name_lut_fname,
                                                            list(nametaxa.lut.keys()),
                                                            delimiter=delimiter)
            # Append taxonkeys/canonical from GBIF taxonkey webservice to name LUT and file
            self._append_resolved_taxkeys(name_dict, name_lut_fname,
                                          name_fails, nametaxa)
            canonical_lut = Lookup.init_from_dict(name_dict,
                                                valtype=VAL_TYPE.STRING,
                                                encoding=ENCODING)

        return canonical_lut


# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=("""Find a GBIF occurrence record from a dataset downloaded
                                     from the GBIF occurrence web service in
                                     Darwin Core format into BISON format.
                                 """))
    parser.add_argument('gbif_occ_file', type=str,
                        help="""
                        Absolute pathname of the input GBIF occurrence file
                        for data transform.  Path contain downloaded GBIF data
                        and metadata.  If the subdirectories 'tmp' and 'out'
                        are not present in the same directory as the raw data,
                        they will be  created for temp and final output files.
                        """)
    parser.add_argument('gbifid', type=str,
                        help="""
                        GBIF identifier for the record to find.
                        """)
    args = parser.parse_args()
    gbif_interp_file = args.gbif_occ_file
    gbifid = args.gbifid

    overwrite = True
    tmpdir = 'tmp'
    outdir = 'out'
    inpath, gbif_fname = os.path.split(gbif_interp_file)

    gr = GBIFReader(inpath, tmpdir, outdir, 'test')

    gr.open_gbif_for_search(gbif_interp_file)
    badids = []

    rec = gr.find_gbif_record(gbifid)

    badids.append(id)
    for bad in badids:
        rec = gr.find_gbif_record(str(bad))
        print('id {} len {}'.format(bad, len(gr._gbif_line)))

    gr.close()
"""
"""
