"""Tests for the helper tool and dependent objects."""
import pytest
import os

from bison.tools.annotate_riis import (
    PARAMETERS as annotate_riis_params, DESCRIPTION as annotate_riis_desc)
from bison.tools._config_parser import (
    build_parser, process_arguments_from_file)
from bison.tools.chunk_large_file import (
    PARAMETERS as chunk_large_file_params, DESCRIPTION as chunk_large_file_desc)

# Also tests functions of bison.providers.riis_data.NNSL and RIISRec
command_meta = [
    (annotate_riis_params, annotate_riis_desc, "../config/annotate_riis.json"),
    (chunk_large_file_params, chunk_large_file_desc, "../config/chunk_large_file.json")
]


class Test_config_parser:
    """Test the config_parser methods used in all CLI tools."""

    # .....................................
    def test_build_parser(self):
        """Test the build_parser method on all CLI tools."""
        for _params, desc, config in command_meta:
            name = os.path.splitext(os.path.basename(config))[0]
            try:
                parser = build_parser(name, desc)
            except Exception as e:
                pytest.fail(f"build_parser failed with {e}")

    # .....................................
    def test_process_arguments_from_file(self):
        """Test the process_arguments_from_file method on all CLI tools."""
        for params, desc, config in command_meta:
            name = os.path.splitext(os.path.basename(config))[0]
            parser = build_parser(name, desc)
            try:
                args = process_arguments_from_file(parser, params)
            except Exception as e:
                pytest.fail(f"process_arguments_from_file failed with {e}")
