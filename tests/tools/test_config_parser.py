"""Tests for the config-file helper tool and dependent objects."""
import os

import pytest

from bison.common.constants import PARAMETERS
from bison.common.log import Logger
from bison.tools._config_parser import process_arguments_from_file

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"


class Test_config_parser:
    """Test the config_parser methods used in all CLI tools."""

    # .....................................
    def test_process_arguments_from_file(self):
        """Test the configuration files for all CLI tools."""
        params = process_arguments_from_file(config_filename, PARAMETERS)
        try:
            valid_keys = list(PARAMETERS["required"].keys())
        except Exception:
            valid_keys = []
        try:
            opt_keys = list(PARAMETERS["optional"].keys())
        except Exception:
            opt_keys = []
        valid_keys.extend(opt_keys)

        for key in params.keys():
            if (
                    not key.startswith("_comment") and
                    not key.startswith("_test_") and
                    not key.startswith("_ignore")
            ):
                if key not in valid_keys:
                    print(f"Key {key} is not present in tool definition")
                    pytest.fail()
    #
    # # .....................................
    # def test_build_parser(self):
    #     """Test the build_parser method on all CLI tools."""
    #     for _params, desc, config_file, test_values in command_meta:
    #         name = os.path.splitext(os.path.basename(config_file))[0]
    #         try:
    #             build_parser(name, desc)
    #         except Exception as e:
    #             pytest.fail(f"build_parser failed with {e}")
