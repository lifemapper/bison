"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.constants import PARAMETERS
from bison.common.log import Logger
from bison.tools._config_parser import process_arguments_from_file

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)


# .............................................................................
class Test_combine_summaries:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    # .....................................
    def test_combine_summaries(self):
        """Test reading an original RIIS file by checking counts."""
        pass
