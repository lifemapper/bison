"""Module containing a tool for parsing a configuration file for argparse."""
import argparse
import json
import os

CONFIG_FILE_PARAMETER = "config_file"
IS_FILE_PARAM = "is_file"
HELP_PARAM = "help"


# .....................................................................................
def build_parser(command, description):
    """Build an argparse.ArgumentParser object for the tool.

    Args:
        command (str): Command for argument parser.
        description (str): Description of command processes.

    Returns:
        argparse.ArgumentParser: An argument parser for the tool's parameters.
    """
    parser = argparse.ArgumentParser(prog=command, description=description)
    parser.add_argument(
        CONFIG_FILE_PARAMETER, type=str, help="Path to configuration file.")
    return parser


# .....................................................................................
def process_arguments_from_file(parser, parameters):
    """Process arguments provided by configuration file.

    Args:
        parser (argparse.ArgumentParser): An argparse.ArgumentParser with parameters.
        parameters (dict): Dictionary of optional and required arguments with expected
            value, and help string.

    Returns:
        argparse.Namespace: An augmented Namespace with any parameters specified in a
            configuration file.

    Raises:
        FileNotFoundError: on non-existent config_file.
        json.decoder.JSONDecodeError: on badly constructed JSON file
        Exception: on missing configuration file argument.
        Exception: on missing required parameter in configuration file.
    """
    args = parser.parse_args()

    # Retrieve arguments from configuration file
    if hasattr(args, CONFIG_FILE_PARAMETER):
        config_filename = getattr(args, CONFIG_FILE_PARAMETER)
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
    else:
        raise Exception("Failed to provide configuration file argument")

    # Test that required arguments are present in configuration file
    try:
        req_args = parameters["required"]
    except Exception:
        req_args = {}
    for key, argdict in req_args.items():
        try:
            val = getattr(args, key)
        except Exception:
            raise Exception(f"Missing required argument {key} in {config_filename}")
        else:
            # Raises an exception if the value is a filename, but does not exist
            test_if_file(val, parameters)

    return args


# .....................................................................................
def test_if_file(val, parameters):
    """If the value is a file, test for its existence.

    Args:
        val (str): value to test if  existence
        parameters (str): Parameters and descriptive metadata

    Raises:
        Exception: on value is a file, and does not exist
    """
    errmsg = None
    try:
        is_file = parameters[IS_FILE_PARAM]
    except Exception:
        pass
    else:
        if is_file is True and not os.path.exists(val):
            try:
                help_str = parameters[HELP_PARAM]
            except Exception:
                help_str = ""
            raise Exception(f"File {val} does not exist: {help_str}.")


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
