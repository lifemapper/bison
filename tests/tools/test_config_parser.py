"""Tests for the config-file helper tool and dependent objects."""
import os

import pytest

from bison.tools._config_parser import process_arguments_from_file
from bison.tools.annotate_riis import DESCRIPTION as annotate_riis_desc
from bison.tools.annotate_riis import PARAMETERS as annotate_riis_params
from bison.tools.chunk_large_file import DESCRIPTION as chunk_large_file_desc
from bison.tools.chunk_large_file import PARAMETERS as chunk_large_file_params
from tests.tools.test_setup import get_test_parameters

command_meta = [
    (annotate_riis_params, annotate_riis_desc, "../../data/config/annotate_riis.json",
     get_test_parameters("test_annotate_riis")),
    (chunk_large_file_params, chunk_large_file_desc,
     "../../data/config/chunk_large_file.json",
     get_test_parameters("test_config_parser"))
]

script_name = os.path.splitext(os.path.basename(__file__))[0]


class Test_config_parser:
    """Test the config_parser methods used in all CLI tools."""

    # .....................................
    def test_process_arguments_from_file(self):
        """Test the configuration files for all CLI tools."""
        for params, _desc, config_file, _test_values in command_meta:
            # This returns exception if missing required parameter
            config = process_arguments_from_file(config_file, params)
            # Test all parameters
            try:
                valid_keys = list(params["required"].keys())
            except Exception:
                valid_keys = []
            try:
                opt_keys = list(params["optional"].keys())
            except Exception:
                opt_keys = []
            valid_keys.extend(opt_keys)

            for key in config.keys():
                if not key.startswith("_comment") and not key.startswith("_test_"):
                    if key not in valid_keys:
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