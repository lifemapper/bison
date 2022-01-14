"""Common file handling tools used in various BISON modules."""
import csv
import sys

from bison.common.constants import ENCODING


# .............................................................................
def get_csv_writer(datafile, delimiter, fmode="w"):
    """Create a CSV writer.

    Args:
        datafile: output CSV file for writing
        delimiter: field separator
        fmode: Write ('w') or append ('a')

    Returns:
        writer (csv.writer) ready to write
        f (file handle)

    Raises:
        Exception: on failure to create a csv writer
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")

    csv.field_size_limit(sys.maxsize)
    try:
        f = open(datafile, fmode, newline="", encoding=ENCODING)
        writer = csv.writer(
            f, escapechar="\\", delimiter=delimiter, quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise e
    else:
        print("Opened file {} for write".format(datafile))
    return writer, f


# .............................................................................
def get_csv_dict_writer(csvfile, header, delimiter, fmode="w"):
    """Create a CSV dictionary writer and write the header.

    Args:
        csvfile: output CSV file for writing
        header: header for output file
        delimiter: field separator
        fmode: Write ('w') or append ('a')

    Returns:
        writer (csv.DictWriter) ready to write
        f (file handle)

    Raises:
        Exception: on invalid file mode
        Exception: on failure to create a DictWriter
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")

    csv.field_size_limit(sys.maxsize)
    try:
        f = open(csvfile, fmode, newline="", encoding=ENCODING)
        writer = csv.DictWriter(f, fieldnames=header, delimiter=delimiter)
    except Exception as e:
        raise e
    else:
        writer.writeheader()
        print("Opened file {} and wrote header".format(csvfile))
    return writer, f


# .............................................................................
if __name__ == "__main__":
    print('sys path = ', sys.path)
