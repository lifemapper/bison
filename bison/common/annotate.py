"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    ENCODING, GBIF, LOG, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, NEW_RIIS_ASSESSMENT_FLD,
    NEW_RIIS_KEY_FLD, RIIS_SPECIES, US_CENSUS_COUNTY, US_CENSUS_STATE, US_STATES)
from bison.common.gbif import DwcData
from bison.common.geoindex import GeoResolver, GeoException
from bison.common.riis import NNSL

from bison.tools.util import (get_csv_dict_writer, get_logger)


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(self, gbif_occ_filename, logger=None):
        """Constructor.

        Args:
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
            logger (object): logger for saving relevant processing messages
        """
        datapath, _ = os.path.split(gbif_occ_filename)
        self._datapath = datapath
        self._csvfile = gbif_occ_filename

        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        riis_filename = os.path.join(datapath, RIIS_SPECIES.FNAME)
        self.nnsl = NNSL(riis_filename, logger=logger)
        self.nnsl.read_riis(read_resolved=True)

        # Must georeference points to add new, consistent state and county fields
        self._geo_county = GeoResolver(US_CENSUS_COUNTY.FILE, US_CENSUS_COUNTY.CENSUS_BISON_MAP, self._log)
        self._geo_state = GeoResolver(US_CENSUS_STATE.FILE, US_CENSUS_STATE.CENSUS_BISON_MAP, self._log)

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

        # Test DwC record contents
        self.good_locations = {}
        self.bad_locations = {}
        self.missing_states = 0
        self.matched_states = 0
        self.mismatched_states = 0

    # ...............................................
    @classmethod
    def construct_annotated_name(cls, csvfile):
        """Construct a filename for the annotated version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename for this data.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        basename, ext = os.path.splitext(csvfile)
        outfname = f"{basename}_annotated{ext}"
        return outfname

    # ...............................................
    @property
    def annotated_dwc_fname(self):
        """Construct a filename for the annotated GBIF DWC file from the original.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        basename, ext = os.path.splitext(self._csvfile)
        outfname = f"{basename}_annotated{ext}"
        return outfname

    # ...............................................
    def _open_input_output(self):
        """Open the DwcData for reading and the csv_writer for writing.

        Also reads the first record and writes the header.

        Raises:
            Exception: on failure to open the DwcData csvreader.
            Exception: on failure to open the csv_writer.
        """
        outfname = self.annotated_dwc_fname
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
                outfname, header, GBIF.DWCA_DELIMITER, fmode="w", encoding=ENCODING,
                overwrite=True)
        except Exception:
            raise Exception(f"Failed to open file or csv_writer for {outfname}")

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
    def assess_occurrence(self, dwcrec, county, state, iis_reclist):
        """Find RIIS assessment matching the acceptedTaxonKey and state in this record.

        Args:
            dwcrec (dict): dictionary of original DwC specimen occurrence record
            county (str): county returned from geospatial intersection of point with YS boundaries
            state (str): state returned from geospatial intersection of point with YS boundaries
            iis_reclist (list of dict): list of RIIS records with acceptedTaxonKey matching the
                acceptedTaxonKey for this occurrence

        Returns:
            riis_assessment: Determination of "introduced" or "invasive" for this
                record with species in this locaation.
            riis_id: locally unique RIIS occurrenceID identifying this determination
                for this species in this location.
        """
        riis_assessment = None
        riis_key = None
        for iisrec in iis_reclist:
            # Double check NNSL dict key == RIIS resolved key == occurrence accepted key
            if dwcrec[GBIF.ACC_TAXON_FLD] != iisrec[RIIS_SPECIES.NEW_GBIF_KEY]:
                self._log.debug("WTF is happening?!?")

            # Look for AK or HI
            if ((state == "AK" and iisrec[RIIS_SPECIES.LOCALITY_FLD] == "AK")
                    or (state == "HI" and iisrec[RIIS_SPECIES.LOCALITY_FLD] == "HI")):
                riis_assessment = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                riis_key = iisrec[RIIS_SPECIES.KEY]

            # Not AK or HI, is it L48?
            elif state in self._conus_states and iisrec[RIIS_SPECIES.LOCALITY_FLD] == "L48":
                riis_assessment = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                riis_key = iisrec[RIIS_SPECIES.KEY]

        if riis_assessment and riis_key:
            self._log.info(f"Adding assessment {riis_assessment} to record {dwcrec[GBIF.ID_FLD]}")

        return riis_assessment, riis_key

    # # ...............................................
    # def _test_state(self, dwcrec, state_code):
    #     state = dwcrec[GBIF.STATE_FLD]
    #     county = dwcrec[GBIF.COUNTY_FLD]
    #     if len(state) == 0:
    #         self.missing_states += 1
    #     else:
    #         # Capitalized state names, uppercase codes
    #         if len(state) == 2:
    #             state = state.upper()
    #         elif len(state) > 2:
    #             state.capitalize()
    #
    #         if state in self._all_states:
    #             # Good state/county combos
    #             try:
    #                 self.good_locations[state].add(county)
    #             except KeyError:
    #                 self.good_locations[dwcrec[GBIF.STATE_FLD]] = set(dwcrec[GBIF.COUNTY_FLD])
    #
    #             # Does record state == georeferenced state?
    #             try:
    #                 scode = US_STATES[state]
    #             except KeyError:
    #                 scode = None
    #             if state == state_code or scode == state_code:
    #                 self.matched_states += 1
    #             else:
    #                 self.mismatched_states += 1
    #
    #         else:
    #             # Bad state/county combos
    #             try:
    #                 self.bad_locations[state].add(county)
    #             except KeyError:
    #                 self.bad_locations[dwcrec[GBIF.STATE_FLD]] = set(dwcrec[GBIF.COUNTY_FLD])

    # ...............................................
    def append_dwca_records(self):
        """Resolve and append state, county, RIIS assessment, and RIIS key to GBIF DWC occurrence records.

        Returns:
            self.annotated_dwc_fname: full filename of the GBIF DWC records with appended fields.

        Raises:
            Exception: on failure to open input or output data.
            Exception: on unexpected failure to read or write data.
        """
        try:
            # Open the original DwC data file for read, and the annotated file for write.
            self._open_input_output()
        except Exception:
            raise
        else:
            # Create geospatial index to identify county/state of points
            self._geo_county.initialize_geospatial_data()
            self._geo_state.initialize_geospatial_data()

            try:
                # iterate over DwC records
                dwcrec = self._dwcdata.get_record()
                while dwcrec is not None:
                    if (self._dwcdata.recno % LOG.INTERVAL) == 0:
                        self._log.debug(f"*** Record number {self._dwcdata.recno} ***")
                    if dwcrec[GBIF.ID_FLD] == "3327828493":
                        print("Here lies a problem")

                    # Initialize new fields
                    county = state = riis_assessment = riis_key = None

                    # Find county and state for these coords
                    try:
                        county, state = self._find_county_state(
                            dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD])
                    except ValueError as e:
                        self._log.error(f"Record {self._dwcdata.recno}: {e}")
                    except GeoException as e:
                        self._log.error(f"Record {self._dwcdata.recno}: {e}")

                    # Find RIIS records for this acceptedTaxonKey
                    taxkey = dwcrec[GBIF.ACC_TAXON_FLD]
                    try:
                        iis_reclist = self.nnsl.data[taxkey]
                    except Exception:
                        iis_reclist = []

                    if county and state and iis_reclist:
                        riis_assessment, riis_key = self.assess_occurrence(
                            dwcrec, county, state, iis_reclist)

                    # Add county, state and RIIS assessment to record
                    dwcrec[NEW_RESOLVED_COUNTY] = county
                    dwcrec[NEW_RESOLVED_STATE] = state
                    dwcrec[NEW_RIIS_ASSESSMENT_FLD] = riis_assessment
                    dwcrec[NEW_RIIS_KEY_FLD] = riis_key

                    try:
                        self._csv_writer.writerow(dwcrec)
                    except ValueError as e:
                        print(f"Error {e} on line {self._dwcdata.recno}")

                    dwcrec = self._dwcdata.get_record()
            except Exception as e:
                raise Exception(f"Unexpected error {e} reading {self._dwcdata.input_file} or writing {self.annotated_dwc_fname}")

        return self.annotated_dwc_fname

    # ...............................................
    def _find_county_state(self, lon, lat):
        county = state = None
        if None not in (lon, lat):
            # Intersect coordinates with county boundaries for state and county values
            try:
                fldvals, ogr_seconds = self._geo_county.find_enclosing_polygon(lon, lat)
            except ValueError:
                raise
            except GeoException:
                raise
            if ogr_seconds > 0.75:
                self._log.debug("Rec {self._dwcdata.recno}; intersect point {lon}, {lat}; OGR time {ogr_seconds}")
            county = fldvals[NEW_RESOLVED_COUNTY]
            state = fldvals[NEW_RESOLVED_STATE]
        return county, state
