"""Tests for the Aggregator processes."""
import os

from bison.common.constants import PARAMETERS
from bison.common.log import Logger
from bison.tools._config_parser import process_arguments_from_file

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)


# .............................................................................
class Test_combine_summaries:
    """Test the tool and dependencies that summarize annotated GBIF data."""

    # .....................................
    def test_combine_summaries(self):
        """Test reading an original RIIS file by checking counts."""
        pass
