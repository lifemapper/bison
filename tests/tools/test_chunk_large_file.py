"""Tests for the chunk_large_file tool and dependent objects."""
import math
import os

from bison.common.constants import PARAMETERS
from bison.common.log import Logger
from bison.common.util import Chunker, count_lines
from bison.tools._config_parser import process_arguments_from_file

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)

CHUNK_COUNT = 10


# .............................................................................
class Test_chunk_large_file:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    # .....................................
    def test_identify_chunks(self):
        """Test identifying the chunks of records to be put into smaller files."""
        chunk_files = Chunker.identify_chunks(
            params["gbif_filename"], chunk_count=params["chunk_count"])
        assert(len(chunk_files) == 10)


    # .....................................
    def test_chunk_files(self):
        """Test chunking a large file into smaller files."""
        chunk_filenames, _report = Chunker.chunk_files(
            params["gbif_filename"], params["output_path"], logger,
            chunk_count=CHUNK_COUNT)

        file_count = len(chunk_filenames)
        assert (file_count == CHUNK_COUNT)

        # The last file may be a smaller size than all the others
        expected_chunk_size = math.ceil(
            params["_test_record_count"] / params["number_of_chunks"])
        name_linecount = []
        for fn in chunk_filenames:
            line_count = count_lines(fn)
            name_linecount.append((fn, line_count - 1))
        rcounts = [tmp[1] for tmp in name_linecount]
        fullsize_chunk_count = rcounts.count(expected_chunk_size)

        # All but possibly the last file should be the chunk size
        assert(
            fullsize_chunk_count == file_count - 1
            or fullsize_chunk_count == file_count)

    # .....................................
    def test_identify_chunk_files(self):
        """Test identifying subset filenames created from chunking a large file."""
        chunk_filenames = Chunker.identify_chunk_files(
            params["big_csv_filename"], params["output_path"],
            chunk_count=params["number_of_chunks"])

        assert (len(chunk_filenames) == params["_test_small_number_of_chunks"])
