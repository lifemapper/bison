"""Module containing a tool for parsing a configuration file for argparse."""
import argparse
import json
import os

from bison.common.constants import COMMANDS, CONFIG_PARAM
from bison.common.log import Logger


# .....................................................................................
def _build_parser(command, description):
    """Build an argparse.ArgumentParser object for the tool.

    Args:
        command (str): Command for argument parser.
        description (str): Description of command processes.

    Returns:
        argparse.ArgumentParser: An argument parser for the tool's parameter_meta.
    """
    parser = argparse.ArgumentParser(prog=command, description=description)
    parser.add_argument(
        f"--{CONFIG_PARAM.FILE}", type=str, help='Path to configuration file.')
    parser.add_argument(
        "command", type=str, choices=COMMANDS, help="Process to execute on data."
    )
    return parser


# .....................................................................................
def _get_command_config_file_arguments(parser):
    """Retrieve the configuration file argument passed through a ArgumentParser.

    Args:
        parser (argparse.ArgumentParser): An argparse.ArgumentParser with parameter_meta.

    Returns:
        config_filename: the configuration file argument passed through the command line
    """
    config_filename = None
    args = parser.parse_args()
    if hasattr(args, CONFIG_PARAM.FILE):
        config_filename = getattr(args, CONFIG_PARAM.FILE)
    cmd = getattr(args, CONFIG_PARAM.COMMAND)
    return cmd, config_filename


# .....................................................................................
def _confirm_val(key, val, paramdict):
    # First check for boolean values
    expected_type = paramdict[CONFIG_PARAM.TYPE]
    if expected_type is bool:
        valtmp = str(val).lower()
        val = False
        if (valtmp in ("yes", "y", "true", "t", "1")):
            val = True
    else:
        try:
            options = paramdict[CONFIG_PARAM.CHOICES]
        except KeyError:
            pass
        else:
            if val not in options:
                raise Exception(
                    f"Value {val} is not in valid options {options} for {key}.")
    return val


# .....................................................................................
def process_arguments_from_file(config_filename, parameter_meta):
    """Process arguments provided by configuration file.

    Args:
        config_filename (str): Full filename of a JSON file with parameter_meta and values.
        parameter_meta (dict): Dictionary of optional and required arguments with expected
            value, and help string.

    Returns:
        argparse.Namespace: An augmented Namespace with any parameter_meta specified in a
            configuration file.

    Raises:
        Exception: on config_filename is None.
        FileNotFoundError: on missing config_filename.
        json.decoder.JSONDecodeError: on badly constructed JSON file
        Exception: on missing configuration file argument.
        Exception: on missing required parameter in configuration file.
    """
    # Retrieve arguments from configuration file
    if config_filename is None:
        raise Exception("Missing required configuration file")

    try:
        with open(config_filename, mode='rt') as in_json:
            config = json.load(in_json)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError:
        raise

    # Test that required arguments are present in configuration file
    try:
        req_meta = parameter_meta["required"]
    except Exception:
        pass
    else:
        for rkey, p_meta in req_meta.items():
            # Confirm required keys are present
            try:
                val = config[rkey]
            except KeyError:
                raise Exception(f"Missing required argument {rkey} in {config_filename}")
            # Confirm value is valid
            try:
                config[rkey] = _confirm_val(rkey, val, p_meta)
            except Exception:
                raise Exception(f"Missing required argument {rkey} in {config_filename}")

    try:
        opt_meta = parameter_meta["optional"]
    except Exception:
        pass
    else:
        for okey, p_meta in opt_meta.items():
            try:
                val = config[okey]
            except KeyError:
                # Add optional argument with empty value to config dictionary
                if p_meta[CONFIG_PARAM.TYPE] is list:
                    config[okey] = []
                else:
                    config[okey] = None
            else:
                config[okey] = _confirm_val(okey, val, p_meta)

    return config


# .....................................................................................
def get_common_arguments(script_name, description, parameter_meta):
    """Get configuration dictionary for a .

    Args:
        script_name (str): basename of the script being executed.
        description (str): Help string for the script being executed.
        parameter_meta (dict): Dictionary of optional and required arguments with expected
            value, and help string.

    Returns:
        config: A parameter/argument dictionary contained in the config_filename.
        logger (bison.common.log.Logger): logger for saving relevant processing messages
        report_filename: optional filename for saving summary process information.

    Raises:
        Exception: on missing --config_file argument

    TODO: make config_file required or accept other parameter_meta from the command line.
    """
    parser = _build_parser(script_name, description)
    command, config_filename = _get_command_config_file_arguments(parser)
    if command not in COMMANDS:
        raise Exception(f"{command} is not in valid commands: {COMMANDS}")
    if not config_filename:
        raise Exception(f"Script {script_name} requires value for config_file")
    config = process_arguments_from_file(config_filename, parameter_meta)
    config["command"] = command
    meta_basename = f"{script_name}_{command}"

    config["report_filename"] = os.path.join(
        config["output_path"], f"{meta_basename}.rpt")
    log_filename = os.path.join(
        config["process_path"], f"{meta_basename}.log")

    logger = Logger(script_name, log_filename=log_filename)

    return config, logger


# .....................................................................................
__all__ = ["get_common_arguments", "process_arguments_from_file"]
