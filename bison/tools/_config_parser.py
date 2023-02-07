"""Module containing a tool for parsing a configuration file for argparse."""
import argparse
import json

from bison.common.constants import CONFIG_PARAM
from bison.common.log import Logger


# .....................................................................................
def _build_parser(command, description):
    """Build an argparse.ArgumentParser object for the tool.

    Args:
        command (str): Command for argument parser.
        description (str): Description of command processes.

    Returns:
        argparse.ArgumentParser: An argument parser for the tool's parameters.
    """
    parser = argparse.ArgumentParser(prog=command, description=description)
    parser.add_argument(
        f"--{CONFIG_PARAM.FILE}", type=str, help='Path to configuration file.')
    return parser


# .....................................................................................
def _get_config_file_argument(parser):
    """Retrieve the configuration file argument passed through a ArgumentParser.

    Args:
        parser (argparse.ArgumentParser): An argparse.ArgumentParser with parameters.

    Returns:
        config_filename: the configuration file argument passed through the command line
    """
    config_filename = None
    args = parser.parse_args()
    if hasattr(args, CONFIG_PARAM.FILE):
        config_filename = getattr(args, CONFIG_PARAM.FILE)
    return config_filename


# .....................................................................................
def process_arguments_from_file(config_filename, parameters):
    """Process arguments provided by configuration file.

    Args:
        config_filename (str): Full filename of a JSON file with parameters and values.
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
    # Retrieve arguments from configuration file
    if config_filename is not None:
        try:
            with open(config_filename, mode='rt') as in_json:
                config = json.load(in_json)
        except FileNotFoundError:
            raise
        except json.decoder.JSONDecodeError:
            raise

    # Test that required arguments are present in configuration file
    try:
        req_args = parameters["required"]
    except Exception:
        req_args = {}
    for key, _argdict in req_args.items():
        try:
            val = config[key]
        except Exception:
            raise Exception(f"Missing required argument {key} in {config_filename}")

    return config


# .....................................................................................
def get_common_arguments(script_name, description, parameters):
    """Get configuration dictionary for a .

    Args:
        script_name (str): basename of the script being executed.
        description (str): Help string for the script being executed.
        parameters (dict): Dictionary of optional and required arguments with expected
            value, and help string.

    Returns:
        config: A parameter/argument dictionary contained in the config_filename.
        logger: logger for saving relevant processing messages
        report_filename: optional filename for saving summary process information.
    """
    parser = _build_parser(script_name, description)
    config_filename = _get_config_file_argument(parser)
    config = process_arguments_from_file(config_filename, parameters)

    try:
        log_filename = config["log_filename"]
    except KeyError:
        log_filename = None
    logger = Logger(script_name, log_filename)

    # If the output report was requested, write it
    try:
        report_filename = config["report_filename"]
    except KeyError:
        report_filename = None

    return config, logger, report_filename


# .....................................................................................
__all__ = ["get_common_arguments", "process_arguments_from_file"]
