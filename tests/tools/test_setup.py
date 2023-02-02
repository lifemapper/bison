"""Parameters for tests."""
import json

test_config_file = "../config/test_parameters.json"


# .............................................................................
def get_parameters(script_name):
    try:
        with open(test_config_file, mode='rt') as in_json:
            args = json.load(in_json)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError:
        raise
    parameters = args[script_name]
    return parameters
