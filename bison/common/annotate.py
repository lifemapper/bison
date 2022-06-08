"""Common classes for adding USGS RIIS info to GBIF occurrences."""
from datetime import datetime
from multiprocessing import Pool, cpu_count
import os

from bison.common.constants import (
    DATA_PATH, ENCODING, GBIF, LOG, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE,
    NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD, NEW_FILTER_FLAG, POINT_BUFFER_RANGE,
    RIIS_SPECIES, US_CENSUS_COUNTY, US_STATES)
from bison.common.gbif import DwcData
from bison.common.riis import NNSL
from bison.tools.geoindex import GeoResolver, GeoException
from bison.tools.util import (get_csv_dict_writer, get_logger)


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(self, gbif_occ_filename, nnsl=None, logger=None):
        """Constructor.

        Args:
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
            nnsl (bison.common.riis.NNSL): object containing USGS RIIS data for annotating records
            logger (object): logger for saving relevant processing messages
        """
        datapath, _ = os.path.split(gbif_occ_filename)
        self._datapath = datapath
        self._csvfile = gbif_occ_filename

        if logger is None:
            logger = get_logger(os.path.join(datapath, LOG.DIR))
        self._log = logger

        if nnsl is not None:
            self.nnsl = nnsl
        else:
            riis_filename = os.path.join(datapath, RIIS_SPECIES.FNAME)
            self.nnsl = NNSL(riis_filename, logger=logger)
            self.nnsl.read_riis(read_resolved=True)

        # Must georeference points to add new, consistent state and county fields
        geofile = os.path.join(US_CENSUS_COUNTY.PATH, US_CENSUS_COUNTY.FILE)
        self._geo_county = GeoResolver(geofile, US_CENSUS_COUNTY.CENSUS_BISON_MAP, self._log)

        # Input reader
        self._dwcdata = DwcData(self._csvfile, logger=logger)
        # Output writer
        self._csv_writer = None

        self._conus_states = []
        for k, v in US_STATES.items():
            if k not in ("Alaska", "Hawaii"):
                self._conus_states.extend([k, v])
        self._all_states = self._conus_states.copy()
        self._all_states.extend(["Alaska", "Hawaii", "AK", "HI"])
        self.bad_ranks = set()
        self.rank_filtered_records = 0

    # ...............................................
    @classmethod
    def construct_annotated_name(cls, csvfile):
        """Construct a full filename for the annotated version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename for this data.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        pth, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        try:
            rawidx = basename.index("_raw")
            basename = basename[:rawidx]
        except ValueError:
            pass
        newbasefilename = f"{basename}_annotated{ext}"
        outfname = os.path.join(pth, newbasefilename)
        return outfname

    # ...............................................
    @classmethod
    def construct_annotated_pattern(cls, csvfile):
        """Construct a full filename pattern matching the annotated file(s) of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename for this data.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        pth, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        try:
            rawidx = basename.index("_raw")
            basename = basename[:rawidx]
        except ValueError:
            pass
        newbasefilename = f"{basename}_annotated{ext}"
        outfname = os.path.join(pth, newbasefilename)
        return outfname

    # ...............................................
    def _open_input_output(self):
        """Open the DwcData for reading and the csv_writer for writing.

        Also reads the first record and writes the header.

        Returns:
            outfname: full filename of the output file

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.
        """
        outfname = self.construct_annotated_name(self._csvfile)
        try:
            self._dwcdata.open()
        except Exception:
            raise

        header = self._dwcdata.fieldnames
        header.extend(
            [NEW_RIIS_KEY_FLD, NEW_RIIS_ASSESSMENT_FLD,
             NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE])

        try:
            self._csv_writer, self._outf = get_csv_dict_writer(
                outfname, header, GBIF.DWCA_DELIMITER, fmode="w", encoding=ENCODING, overwrite=True)
        except Exception:
            raise Exception(f"Failed to open file or csv_writer for {outfname}")

        return outfname

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        self._dwcdata.close()
        try:
            self._outf.close()
            self._csv_writer = None
        except AttributeError:
            pass

    # ...............................................
    @property
    def is_open(self):
        """Return true if any files are open.

        Returns:
            :type bool, True if CSV file is open, False if CSV file is closed
        """
        if ((self._inf is not None and not self._inf.closed)
                or (self._outf is not None and not self._outf.closed)):
            return True
        return False

    # ...............................................
    def annotate_one_record(self, dwcrec):
        """Append fields to GBIF record, then write to file.

        Args:
            dwcrec: one original GBIF DwC record

        Returns:
            dwcrec: one annotated GBIF DwC record
        """
        gbif_id = dwcrec[GBIF.ID_FLD]
        if (self._dwcdata.recno % LOG.INTERVAL) == 0:
            self._log.info(
                f"*** Record number {self._dwcdata.recno}, gbifID: {gbif_id} ***")

        # Find county and state for these coords
        county = state = riis_assessment = riis_key = None
        try:
            county, state = self._find_county_state(
                dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD],
                buffer_vals=POINT_BUFFER_RANGE)
        except ValueError as e:
            self._log.error(f"Record gbifID: {gbif_id}: {e}")
        except GeoException as e:
            self._log.error(f"Record gbifID: {gbif_id}: {e}")

        region = "L48"
        if state in ("AK", "HI"):
            region = state

        # Identify whether this record is above Species rank
        # (exclude higher level determinations from resolution and summary)
        dwcrec[NEW_FILTER_FLAG] = "true"
        if dwcrec[GBIF.RANK_FLD].lower() not in GBIF.ACCEPT_RANK_VALUES:
            dwcrec[NEW_FILTER_FLAG] = "false"
            self.bad_ranks.add(dwcrec[GBIF.RANK_FLD])
            self.rank_filtered_records += 1
        # Species or below
        else:
            # Find RIIS records for this acceptedTaxonKey.
            taxkeys = [dwcrec[GBIF.ACC_TAXON_FLD]]
            if (dwcrec[GBIF.RANK_FLD].lower() != "species"
                    and dwcrec[GBIF.ACC_TAXON_FLD] != dwcrec[GBIF.SPECIES_KEY_FLD]):
                # If acceptedTaxonKey is below species, find RIIS records for species
                # key too.
                taxkeys.append(dwcrec[GBIF.SPECIES_KEY_FLD])

            # Find RIIS records for this acceptedTaxonKey.  If  acceptedTaxonKey
            # taxkey = dwcrec[GBIF.ACC_TAXON_FLD]
            (riis_assessment, riis_key) = self.nnsl.get_assessment_for_gbif_taxonkeys_region(
                taxkeys, region)
        #
        # # Determine whether to include this in summaries
        # dwcrec[NEW_FILTER_FLAG] = "true"
        # if dwcrec[GBIF.RANK_FLD] not in GBIF.ACCEPT_RANK_VALUES:
        #     dwcrec[NEW_FILTER_FLAG] = "false"
        #     self.bad_ranks.add(dwcrec[GBIF.RANK_FLD])
        #     self.rank_filtered_records += 1

        # Add county, state and RIIS assessment to record
        dwcrec[NEW_RESOLVED_COUNTY] = county
        dwcrec[NEW_RESOLVED_STATE] = state
        dwcrec[NEW_RIIS_ASSESSMENT_FLD] = riis_assessment
        dwcrec[NEW_RIIS_KEY_FLD] = riis_key

        return dwcrec

    # ...............................................
    def annotate_dwca_records(self):
        """Resolve and append state, county, RIIS assessment, and RIIS key to GBIF DWC occurrence records.

        Returns:
            self.annotated_dwc_fname: full filename of the GBIF DWC records with appended fields.

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        try:
            # Open the original DwC data file for read, and the annotated file for write.
            annotated_dwc_fname = self._open_input_output()
        except Exception:
            raise
        else:
            self._log.info(f"Annotating {self._csvfile} to create {annotated_dwc_fname}")
            try:
                # iterate over DwC records
                dwcrec = self._dwcdata.get_record()
                while dwcrec is not None:
                    # Annotate
                    dwcrec_ann = self.annotate_one_record(dwcrec)
                    # Write
                    try:
                        self._csv_writer.writerow(dwcrec_ann)
                    except ValueError as e:
                        self._log.error(
                            f"ValueError {e} on record, gbifID {dwcrec[GBIF.ID_FLD]}")
                    except Exception as e:
                        self._log.error(
                            f"Unknown error {e} record, gbifID {dwcrec[GBIF.ID_FLD]}")
                    # Get next
                    dwcrec = self._dwcdata.get_record()
            except Exception as e:
                raise Exception(
                    f"Unexpected error {e} reading {self._dwcdata.input_file} or "
                    + f"writing {annotated_dwc_fname}")

        self._log.info(
            f"Annotate records filtered out {self.rank_filtered_records} " +
            f"records with {self.bad_ranks} ranks")

        return annotated_dwc_fname

    # ...............................................
    def _find_county_state(self, lon, lat, buffer_vals):
        county = state = None
        if None not in (lon, lat):
            # Intersect coordinates with county boundaries for state and county values
            try:
                fldvals, ogr_seconds = self._geo_county.find_enclosing_polygon(lon, lat, buffer_vals=buffer_vals)
            except ValueError:
                raise
            except GeoException:
                raise
            if ogr_seconds > 0.75:
                self._log.debug(f"Rec {self._dwcdata.recno}; intersect point {lon}, {lat}; OGR time {ogr_seconds}")
            county = fldvals[NEW_RESOLVED_COUNTY]
            state = fldvals[NEW_RESOLVED_STATE]
        return county, state


# .............................................................................
def annotate_occurrence_file(input_filename):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        input_filename (str): full filename containing GBIF data for annotation.

    Returns:
        annotated_dwc_fname: full filename for GBIF data annotated with state, county, RIIS assessment, and RIIS key.

    Raises:
        FileNotFoundError: on missing input file
    """
    if not os.path.exists(input_filename):
        raise FileNotFoundError(input_filename)

    datapath, basefname = os.path.split(input_filename)
    logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{basefname}")
    logger.info(f"Submit {basefname} for annotation")

    orig_riis_filename = os.path.join(DATA_PATH, RIIS_SPECIES.FNAME)
    nnsl = NNSL(orig_riis_filename, logger=logger)
    nnsl.read_riis(read_resolved=True)

    logger.info("Start Time : {}".format(datetime.now()))
    ant = Annotator(input_filename, nnsl=nnsl, logger=logger)
    annotated_dwc_fname = ant.annotate_dwca_records()
    logger.info("End Time : {}".format(datetime.now()))
    return annotated_dwc_fname


# .............................................................................
def parallel_annotate(input_filenames, main_logger):
    """Main method for parallel execution of DwC annotation script.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for annotation.
        main_logger (logger): logger for the process that calls this function, initiating subprocesses

    Returns:
        annotated_dwc_fnames (list): list of full output filenames
    """
    infiles = []
    # Process only needed files
    for in_csv in input_filenames:
        out_csv = Annotator.construct_annotated_name(in_csv)
        if os.path.exists(out_csv):
            main_logger.info(f"Annotations exist in {out_csv}, moving on.")
        else:
            infiles.append(in_csv)

    main_logger.info("Parallel Annotation Start Time : {}".format(datetime.now()))
    # Do not use all CPUs
    pool = Pool(cpu_count() - 2)
    # Map input files asynchronously onto function
    map_result = pool.map_async(annotate_occurrence_file, infiles)
    # Wait for results
    map_result.wait()
    annotated_dwc_fnames = map_result.get()
    main_logger.info("Parallel Annotation End Time : {}".format(datetime.now()))

    return annotated_dwc_fnames
