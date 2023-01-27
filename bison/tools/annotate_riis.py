from bison.tools._config_parser import build_parser, process_arguments

COMMAND = "annotate_riis"
DESCRIPTION = """\
Annotate a CSV file containing the USGS Registry for Introduced and Invasive 
Species (RIIS) with accepted names from GBIF. """
ARGUMENTS = {
    "required":
        {
            "riis_filename":
                {
                    "type": str,
                    "test_file_existence": True,
                    "help": "Filename of the most current USGS RIIS Master List"
                },
            "annotated_riis_filename":
                {
                    "type": str,
                    "help": "Filename to write the annotated RIIS list."
                },
    "optional":
        {
            "log_filename":
                {
                    "type": str,
                    "help": "Filename to write logging data."},
            "log_console":
                {
                    "type": bool,
                    "help": "`true` to write logging statements to console."
                },
            "report_filename":
                {
                    "type": str,
                    "help": "Filename to write summary metadata."}

        }}
}


"""
Note: 
The tool requires a configuration file in JSON format, which has the following  
required and optional parameters: 

Required:
    riis_filename: full path to the most current USGS RIIS Master List 
    annotated_riis_filename: full path for writing the annotated RIIS list

Optional:
    log_filename: A file location to write logging data.
    log_console: If provided, write logging statements to console.
    report_filename: A file location to write summary metadata.
"""


# .....................................................................................
def cli():
    """Command-line interface to build grid.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    parser = build_parser(COMMAND, DESCRIPTION)
    args = process_arguments(parser, config_arg='config_file')



# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
