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
def get_csv_dict_reader(csvfile, delimiter, fieldnames=None, encoding=ENCODING, ignore_quotes=False):
    """Create a CSV dictionary writer and write the header.

    Args:
        csvfile: output CSV file for reading
        delimiter: delimiter between fields
        fieldnames (list): if header is not in the file, use these fieldnames
        encoding (str): type of encoding
        ignore_quotes (constant): csv.QUOTE_NONE or csv.QUOTE_MINIMAL

    Returns:
        writer (csv.DictReader) ready to read
        f (file handle)

    Raises:
        Exception: on failure to open file
        Exception: on failure to create a DictWriter
    """
    try:
        #  If csvfile is a file object, it should be opened with newline=""
        f = open(csvfile, "r", newline="", encoding=encoding)
    except Exception as e:
        raise e

    if fieldnames is None:
        try:
            header = next(f)
            tmpflds = header.split(delimiter)
            fieldnames = [fld.strip() for fld in tmpflds]
        except Exception as e:
            raise e

    # QUOTE_NONE or QUOTE_MINIMAL
    if ignore_quotes:
        dreader = csv.DictReader(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_NONE,
            escapechar='\\', delimiter=delimiter)
    else:
        dreader = csv.DictReader(
            f, fieldnames=fieldnames,
            escapechar='\\', delimiter=delimiter)

        print('Opened file {} for dict read'.format(csvfile))
    return dreader, f



# .............................................................................
if __name__ == "__main__":
    print('sys path = ', sys.path)
