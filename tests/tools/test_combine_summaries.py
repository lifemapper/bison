"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.log import Logger

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_combine_summaries:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    def __init__(self):
        """Constructor."""
        self._logger = Logger(script_name)

    # .....................................
    def test_combine_summaries(self):
        """Test reading an original RIIS file by checking counts."""
        pass
