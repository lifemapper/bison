"""Read test parameters from test JSON configuration file, indexed by script_name."""
import json

test_config_file = "../config/test_parameters.json"


# .............................................................................
def get_test_parameters(script_name):
    """Read arguments from a JSON configuration file.

    Args:
        script_name (str): Name of script using these .

    Returns:
        dict: A dictionary of key/value poirs of parameters/arguments for a test script.

    Raises:
        FileNotFoundError: on missing test_config_file.
        json.decoder.JSONDecodeError: on non-json-parsable test_config_file.
    """
    try:
        with open(test_config_file, mode='rt') as in_json:
            args = json.load(in_json)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError:
        raise
    parameters = args[script_name]
    return parameters
