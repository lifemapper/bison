"""Common classes for adding USGS RIIS info to GBIF occurrences."""
from logging import DEBUG, ERROR
import os
from datetime import datetime
# from multiprocessing import Pool, cpu_count

from bison.common.constants import (
    APPEND_TO_DWC, APPEND_TO_RIIS, ENCODING, GBIF, LOG, POINT_BUFFER_RANGE,
    US_AIANNH, US_CENSUS_COUNTY, US_DOI, US_PAD, US_STATES)
from bison.common.util import get_csv_dict_writer
from bison.process.geoindex import GeoResolver
from bison.providers.gbif_data import DwcData
from bison.providers.riis_data import RIIS


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(
            self, geo_input_path, logger, riis_with_gbif_filename=None, riis=None,
            gbif_occ_filename=None, output_occ_filename=None):
        """Constructor.

        Args:
            geo_input_path (str): input path for geospatial files to intersect points
            logger (object): logger for saving relevant processing messages
            riis_with_gbif_filename (str): full filename of RIIS data annotated with
                GBIF accepted taxon names.
            riis (bison.common.riis.RIIS): object containing USGS RIIS data for
                annotating records
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
            output_occ_filename (str): destination directory and filename to write
                annotated CSV file.

        Raises:
            Exception: on RIIS without annotations provided, and no
                annotated_riis_filename for writing an annotated RIIS file.
            Exception: on neither annotated_riis_filename nor RIIS provided

        Notes:
            constructor creates spatial indices for the geospatial files for
                geographic areas of interest.
            geographic areas are in bison.common.constants variables and include:
                state and county from census boundaries in US_CENSUS_COUNTY
                region names from American Indian/Alaska Native Areas/Hawaiian Home Lands
                    in US_AIANNH
                protected areas from the Protected areas Database in US_PAD
            US-RIIS determinations are calculated from the species/state combination.
        """
        self._log = logger
        self._datapath = None
        self._dwcdata = None
        self._csv_writer = None
        self.initialize_input_output_occurrences(
            gbif_occ_filename, output_occ_filename)

        if riis is not None:
            self.riis = riis
            if self.riis.is_annotated is False:
                if riis_with_gbif_filename is None:
                    raise Exception(
                        "RIIS data does not contain GBIF accepted taxa, needed for "
                        "annotating GBIF occurrence data with RIIS Introduced and "
                        "Invasive determinations.  Provide a riis_with_gbif_filename "
                        "output file to initiate resolution of RIIS records.")
                else:
                    self.riis.resolve_riis_to_gbif_taxa(
                        riis_with_gbif_filename, overwrite=True)
        elif riis_with_gbif_filename is not None:
            self.riis = RIIS(riis_with_gbif_filename, logger, is_annotated=True)
            self.riis.read_riis()
        else:
            raise Exception(
                "Must provide either RIIS with annotations or riis_with_gbif_filename")

        # Geospatial index for consistent state and county values
        county_filename = os.path.join(geo_input_path, US_CENSUS_COUNTY.FILE)
        self._geo_county = GeoResolver(
            county_filename, US_CENSUS_COUNTY.GEO_BISON_MAP, self._log)

        # Geospatial index for Native lands values
        aianhh_filename = os.path.join(geo_input_path, US_AIANNH.FILE)
        self._geo_aianhh = GeoResolver(
            aianhh_filename, US_AIANNH.GEO_BISON_MAP, self._log)

        # Geospatial index for DOI region (to get correct PAD DOI file)
        doi_filename = os.path.join(geo_input_path, US_DOI.FILE)
        self._geo_doi = GeoResolver(
            doi_filename, US_DOI.GEO_BISON_MAP, self._log)

        # Geospatial indices for each DOI region for PAD values
        self._geo_pads = {}
        for region, pad_fn in US_PAD.FILES:
            pad_filename = os.path.join(geo_input_path, pad_fn)
            self._geo_pads[region] = GeoResolver(
                pad_filename, US_PAD.GEO_BISON_MAP, self._log)

        self._conus_states = []
        for k, v in US_STATES.items():
            if k not in ("Alaska", "Hawaii"):
                self._conus_states.extend([k, v])
        self._all_states = self._conus_states.copy()
        self._all_states.extend(["Alaska", "Hawaii", "AK", "HI"])
        self.bad_ranks = set()
        self.rank_filtered_records = 0

    # ...............................................
    def initialize_input_output_occurrences(
            self, gbif_occ_filename, output_occ_filename):
        """Initialize and open required input and output files of occurrence records.

        Also reads the first record and writes the header.

        Args:
            gbif_occ_filename:
            output_occ_filename:

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.
        """
        self.close()
        if gbif_occ_filename is not None:
            if not os.path.exists(gbif_occ_filename):
                raise Exception(f"Missing input occurrence data {gbif_occ_filename}")
            if output_occ_filename is None:
                raise Exception(
                    "Must provide an output filename to write annotated input "
                    f"data in {gbif_occ_filename}")

            self._datapath, _ = os.path.split(gbif_occ_filename)
            self._csvfile = gbif_occ_filename
            self._output_csvfile = output_occ_filename
            # Input reader
            self._dwcdata = DwcData(self._csvfile, logger=self._log)
            try:
                self._dwcdata.open()
            except Exception:
                raise

            header = self._dwcdata.fieldnames
            for fld in APPEND_TO_DWC.annotation_fields():
                header.append(fld)
            try:
                self._csv_writer, self._outf = get_csv_dict_writer(
                    self._output_csvfile, header, GBIF.DWCA_DELIMITER, fmode="w",
                    encoding=ENCODING, overwrite=True)
            except Exception:
                raise Exception(
                    f"Failed to open file or csv_writer for {self._output_csvfile}")

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        self._csv_writer = None
        try:
            self._dwcdata.close()
        except AttributeError:
            pass
        try:
            self._outf.close()
        except AttributeError:
            pass

    # ...............................................
    @property
    def is_open(self):
        """Return true if input or output files are open.

        Returns:
            :type bool, True if a file is open, False if not
        """
        if ((self._dwcdata is not None and not self._dwcdata.closed)
                or (self._outf is not None and not self._outf.closed)):
            return True
        return False

    # ...............................................
    def _filter_find_taxon_keys(self, dwcrec):
        """Returns acceptedTaxonKey for species and a lower rank key where determined.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            taxkeys (list of int): if the record is Species rank or below, return the
                accepted species GBIF taxonKey and, if lower than species, the lower
                rank acceptedTaxonKey.
        """
        taxkeys = []
        # Identify whether this record is above Species rank
        # (exclude higher level determinations from resolution and summary)
        if dwcrec[GBIF.RANK_FLD].lower() not in GBIF.ACCEPT_RANK_VALUES:
            self.bad_ranks.add(dwcrec[GBIF.RANK_FLD])
            self.rank_filtered_records += 1
        # Species or below
        else:
            # Find RIIS records for this acceptedTaxonKey.
            taxkeys.append(dwcrec[GBIF.ACC_TAXON_FLD])
            if (dwcrec[GBIF.RANK_FLD].lower() != "species"
                    and dwcrec[GBIF.ACC_TAXON_FLD] != dwcrec[GBIF.SPECIES_KEY_FLD]):
                # If acceptedTaxonKey is below species, find RIIS records for species
                # key too.
                taxkeys.append(dwcrec[GBIF.SPECIES_KEY_FLD])

        return taxkeys

    # ...............................................
    def _annotate_one_record(self, dwcrec):
        """Append fields to GBIF record, then write to file.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            dwcrec: one GBIF DwC record, if it is not filtered out, values are added
                to calculated fields: APPEND_TO_DWC, otherwise these fields are
                None.
        """
        gbif_id = dwcrec[GBIF.ID_FLD]
        if (self._dwcdata.recno % LOG.INTERVAL) == 0:
            self._log.info(
                f"*** Record number {self._dwcdata.recno}, gbifID: {gbif_id} ***")

        # Leave these fields None if the record is filtered out (rank above species)
        filtered_taxkeys = self._filter_find_taxon_keys(dwcrec)

        # Only append additional values to records that pass the filter tests.
        if filtered_taxkeys:
            lon = dwcrec[GBIF.LON_FLD]
            lat = dwcrec[GBIF.LAT_FLD]

            for georesolver, buffers in (
                    (self._geo_county, POINT_BUFFER_RANGE),
                    (self._geo_aianhh, ()),
                    (self._geo_doi, ())
            ):
                # Find enclosing region and its attributes
                try:
                    fldvals = georesolver.find_enclosing_polygon_attributes(
                        lon, lat, buffer_vals=buffers)
                except ValueError as e:
                    self._log.error(f"Record gbifID: {gbif_id}: {e}")
                else:
                    # Add values to record
                    for fld, val in fldvals.items():
                        dwcrec[fld] = val

            # Find RIIS region from resolved state
            state = dwcrec[APPEND_TO_DWC.RESOLVED_ST]
            riis_region = "L48"
            if state in ("AK", "HI"):
                riis_region = state

            # Find any RIIS records for these acceptedTaxonKeys and region.
            (riis_assessment,
             riis_key) = self.riis.get_assessment_for_gbif_taxonkeys_region(
                filtered_taxkeys, riis_region)
            dwcrec[APPEND_TO_DWC.RIIS_ASSESSMENT] = riis_assessment
            dwcrec[APPEND_TO_DWC.RIIS_KEY] = riis_key

            # Find PAD area from PAD data for the DOI region
            doi_region = dwcrec[APPEND_TO_DWC.DOI_REGION]
            try:
                pad_resolver = self._geo_pads[doi_region]
            except KeyError:
                self._log.log(f"No PAD datafile exists for DOI region {doi_region}")
            else:
                # Find enclosing PAD if exists, and its attributes
                try:
                    fldvals = pad_resolver.find_enclosing_polygon_attributes(
                        lon, lat, buffer_vals=POINT_BUFFER_RANGE)
                except ValueError as e:
                    self._log.error(f"Record gbifID: {gbif_id}: {e}")
                else:
                    # Add values to record
                    for fld, val in fldvals.items():
                        dwcrec[fld] = val

        return dwcrec

    # ...............................................
    def annotate_dwca_records(self, dwc_with_geo_and_riis_filename):
        """Resolve and append state, county, RIIS assessment and key to GBIF records.

        Args:
            dwc_with_geo_and_riis_filename: full filename for writing DWC records with
                RIIS appended fields.

        Returns:
            report: dictionary of metadata about the data and process

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        report = {
            "dwc_filename": self._csvfile,
            "dwc_with_geo_and_riis_filename": dwc_with_geo_and_riis_filename,
            "record_failed_gbifids": []
        }
        # try:
        #     # Open the original DwC data file for read, annotated file for write.
        #     self._open_input_output(dwc_with_geo_and_riis_filename)
        # except Exception:
        #     raise
        # else:
        self._log.log(
            f"Annotating {self._csvfile} to create {dwc_with_geo_and_riis_filename}",
            refname=self.__class__.__name__)
        try:
            # iterate over DwC records
            dwcrec = self._dwcdata.get_record()
            while dwcrec is not None:
                # Annotate
                dwcrec_ann = self._annotate_one_record(dwcrec)
                # Write
                try:
                    self._csv_writer.writerow(dwcrec_ann)
                except Exception as e:
                    self._log.log(
                        f"Error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}",
                        refname=self.__class__.__name__, log_level=ERROR)
                    report["record_failed_gbifids"].append(dwcrec[GBIF.ID_FLD])
                # Get next
                dwcrec = self._dwcdata.get_record()
        except Exception as e:
            raise Exception(
                f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                + f"writing {dwc_with_geo_and_riis_filename}")

        report["bad_ranks_filtered"] = list(self.bad_ranks)
        report["records_filtered_by_rank"] = self.rank_filtered_records
        self._log.log(
            f"Annotate records filtered out {self.rank_filtered_records} " +
            f"records with {self.bad_ranks} ranks", refname=self.__class__.__name__)
        return report


# # .............................................................................
# def parallel_annotate(input_filenames, main_logger):
#     """Main method for parallel execution of DwC annotation script.
#
#     Args:
#         input_filenames (list): list of full filenames containing GBIF data for
#             annotation.
#         main_logger (logger): logger for the process that calls this function,
#             initiating subprocesses
#
#     Returns:
#         annotated_dwc_fnames (list): list of full output filenames
#     """
#     refname = "parallel_annotate"
#     inputs = []
#     # Process only needed files
#     for in_csv in input_filenames:
#         out_csv = Annotator.construct_annotated_name(in_csv)
#         if os.path.exists(out_csv):
#             main_logger.log(
#                 f"Annotations exist in {out_csv}, moving on.", refname=refname)
#         else:
#             inputs.append((in_csv, main_logger.log_directory))
#
#     main_logger.log(
#         "Parallel Annotation Start Time : {}".format(datetime.now()), refname=refname)
#     # Do not use all CPUs
#     pool = Pool(cpu_count() - 2)
#     # Map input files asynchronously onto function
#     # map_result = pool.map_async(annotate_occurrence_file, inputs)
#     map_result = pool.starmap_async(annotate_occurrence_file, inputs)
#     # Wait for results
#     map_result.wait()
#     annotated_dwc_fnames = map_result.get()
#     main_logger.log(
#         "Parallel Annotation End Time : {}".format(datetime.now()), refname=refname)
#
#     return annotated_dwc_fnames
