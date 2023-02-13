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
        params = parameters["required"]
    except Exception:
        params = {}
    for key, _paramdict in params.items():
        try:
            _ = config[key]
        except Exception:
            raise Exception(f"Missing required argument {key} in {config_filename}")

    try:
        opt_args = parameters["optional"]
    except Exception:
        opt_args = {}
    params.update(opt_args)

    # # Test some arguments for existence
    # for key, paramdict in params.items():
    #     # Test existence of input directory
    #     try:
    #         test_in_dir = paramdict[CONFIG_PARAM.IS_INPUT_DIR]
    #         inpath = config[key]
    #     except KeyError:
    #         test_in_dir = False
    #     else:
    #         # Test existence of input file
    #         try:
    #             test_file = paramdict[CONFIG_PARAM.IS_INPUT_FILE]
    #             fname = config[key]
    #         except KeyError:
    #             test_file = False
    #
    #         if test_in_dir:
    #             if not os.path.exists(inpath):
    #                 raise Exception(f"Input path {inpath} does not exist")
    #             in_fname = os.path.join(inpath, fname)
    #             if test_file and not os.path.exists(in_fname):
    #                 raise Exception(f"Input file {in_fname} does not exist")
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
