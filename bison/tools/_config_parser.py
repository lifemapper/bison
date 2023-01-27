"""Module containing a tool for parsing a configuration file for argparse."""
import argparse
import json
import os.path
import sys


# .....................................................................................
def build_parser(command, description):
    """Build an argparse.ArgumentParser object for the tool.

    Returns:
        argparse.ArgumentParser: An argument parser for the tool's parameters.
    """
    parser = argparse.ArgumentParser(prog=command, description=description)
    parser.add_argument("config_file", type=str, help="Path to configuration file.")
    return parser


# .....................................................................................
def process_arguments(parser, config_arg=None):
    """Process arguments provided by configuration file.

    Args:
        parser (argparse.ArgumentParser): An argparse.ArgumentParser with parameters.
        config_arg (str): If provided, try to read configuration file for additional
            arguments.

    Returns:
        argparse.Namespace: An augmented Namespace with any parameters specified in a
            configuration file.

    Raises:
        FileNotFoundError: on non-existent config_file.
        json.decoder.JSONDecodeError: on badly constructed JSON file
    """
    args = parser.parse_args()

    if config_arg is not None and hasattr(args, config_arg):
        config_filename = getattr(args, config_arg)
        if config_filename is not None:
            try:
                with open(config_filename, mode='rt') as in_json:
                    config = json.load(in_json)
                    for k in config.keys():
                        # Always replace existing values
                        setattr(args, k, config[k])
            except FileNotFoundError:
                raise
            except json.decoder.JSONDecodeError:
                raise
    return args

# .....................................................................................
def test_files(*filename_filefunction):
    """Get a logger object (or None) for the provided parameters.

    Args:
        filename_filefunction (str): One or more filename/filefunction tuples

    Returns:
        A message indicating missing files.  If all exist, the message is empty string.
    """
    err_msgs = []
    for filename, filefunction in filename_filefunction:
        if not os.path.exists(filename):
            err_msgs.append(f"File {filename}, {filefunction}, does not exist.")
    return err_msgs
