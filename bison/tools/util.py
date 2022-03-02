"""Common file handling tools used in various BISON modules."""
import csv
import logging
from logging.handlers import RotatingFileHandler
from multiprocessing import cpu_count
import os
import subprocess
import sys
import time

from bison.common.constants import DATA_PATH, ENCODING, GBIF, LOG


# ...............................................
def delete_file(file_name, delete_dir=False):
    """Delete file if it exists, optionally delete newly empty directory.

    Args:
        file_name (str): full path to the file to delete
        delete_dir (bool): flag - True to delete parent directory if it becomes empty

    Returns:
        True if file was not found, or file (and optional newly-empty parent directories) was successfully deleted.
        False if failed to delete file (and parent directories).
    """
    success = True
    msg = ''
    if file_name is None:
        msg = 'Cannot delete file \'None\''
    else:
        pth, _ = os.path.split(file_name)
        if file_name is not None and os.path.exists(file_name):
            try:
                os.remove(file_name)
            except Exception as e:
                success = False
                msg = 'Failed to remove {}, {}'.format(file_name, str(e))
            if delete_dir and len(os.listdir(pth)) == 0:
                try:
                    os.removedirs(pth)
                except Exception as e:
                    success = False
                    msg = 'Failed to remove {}, {}'.format(pth, str(e))
    return success, msg


# ...............................................
def ready_filename(fullfilename, overwrite=True):
    """Delete file if it exists, optionally delete newly empty directory.

    Args:
        fullfilename (str): full path of the file to check
        overwrite (bool): flag indicating whether to delete the file if it already exists

    Returns:
        True if file was not found, or file (and optional newly-empty parent directories) was successfully deleted.
        False if failed to delete file (and parent directories).

    Raises:
        PermissionError: if unable to delete existing file when overwrite is true
        Exception: on other delete errors or failure to create directories
        PermissionError: if unable to create missing directories
        Exception: on other mkdir errors
        Exception: on failure to create directories
    """
    is_ready = True
    if os.path.exists(fullfilename):
        if overwrite:
            try:
                delete_file(fullfilename)
            except PermissionError:
                raise
            except Exception as e:
                raise Exception('Unable to delete {} ({})'.format(fullfilename, e))
        else:
            is_ready = False
    else:
        pth, _ = os.path.split(fullfilename)
        try:
            os.makedirs(pth)
        except FileExistsError:
            pass
        except PermissionError:
            raise
        except Exception:
            raise

        if not os.path.isdir(pth):
            raise Exception('Failed to create directories {}'.format(pth))

    return is_ready


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
    return writer, f


# .............................................................................
def get_csv_dict_writer(csvfile, header, delimiter, fmode="w", encoding=ENCODING, overwrite=True):
    """Create a CSV dictionary writer and write the header.

    Args:
        csvfile (str): output CSV filename for writing
        header (list): header for output file
        delimiter (str): field separator
        fmode (str): Write ('w') or append ('a')
        encoding (str): Encoding for output file
        overwrite (bool): True to delete an existing file before write

    Returns:
        writer (csv.DictWriter) ready to write
        f (file handle)

    Raises:
        Exception: on invalid file mode
        Exception: on failure to create a DictWriter
        FileExistsError: on existing file if overwrite is False
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")
    if ready_filename(csvfile, overwrite=overwrite):
        csv.field_size_limit(sys.maxsize)
        try:
            f = open(csvfile, fmode, newline="", encoding=encoding)
            writer = csv.DictWriter(f, fieldnames=header, delimiter=delimiter)
        except Exception as e:
            raise e
        else:
            writer.writeheader()
        return writer, f
    else:
        raise FileExistsError


# .............................................................................
def get_csv_dict_reader(
        csvfile, delimiter, fieldnames=None, encoding=ENCODING, ignore_quotes=True):
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
        FileNotFoundError: on missing csvfile
        PermissionError: on improper permissions on csvfile
        Exception: on failure to parse first line into fieldnames
    """
    try:
        #  If csvfile is a file object, it should be opened with newline=""
        f = open(csvfile, "r", newline="", encoding=encoding)
    except FileNotFoundError:
        raise
    except PermissionError:
        raise

    if fieldnames is None:
        try:
            header = next(f)
            tmpflds = header.split(delimiter)
            fieldnames = [fld.strip() for fld in tmpflds]
        except Exception:
            raise

    # QUOTE_NONE or QUOTE_MINIMAL
    if ignore_quotes:
        dreader = csv.DictReader(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_NONE,
            escapechar='\\', delimiter=delimiter)
    else:
        dreader = csv.DictReader(
            f, fieldnames=fieldnames,
            escapechar='\\', delimiter=delimiter)

    return dreader, f


# .............................................................................
def get_logger(outpath, logname=None):
    """Get a logger that logs to file and to console in an established format.

    Args:
        outpath (str): path for the logfile
        logname: name for the logger and basename for the logfile.

    Returns:
         a logger
    """
    level = logging.DEBUG
    if logname is not None:
        logfname = os.path.join(outpath, logname + '.log')
    else:
        # get log filename
        logname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logfname = os.path.join(outpath, logname + '_{}.log'.format(timestamp))
    ready_filename(logfname, overwrite=True)

    # get logger
    log = logging.getLogger(logname)
    log.setLevel(level)

    # create file handlers
    handlers = []
    handlers.append(RotatingFileHandler(
        logfname, maxBytes=LOG.FILE_MAX_BYTES, backupCount=LOG.FILE_BACKUP_COUNT))
    handlers.append(logging.StreamHandler())
    # set handlers
    formatter = logging.Formatter(LOG.FORMAT, LOG.DATE_FORMAT)
    for fh in handlers:
        fh.setLevel(level)
        fh.setFormatter(formatter)
        log.addHandler(fh)
    return log


# ...............................................
def logit(logger, msg):
    """Log a message to the console or file.

    Args:
        logger (object): Logger for writing messages to file and console.
        msg (str): Message to be written.
    """
    if logger is not None:
        logger.info(msg)
    else:
        print(msg)

# .............................................................................
def get_header(filename):
    """Find fieldnames from the first line of a CSV file.

    Args:
         filename (str): Full filename for a CSV file with a header.

    Returns:
         header (list): list of fieldnames in the first line of the file
    """
    header = None
    try:
        f = open(filename, 'r', encoding='utf-8')
        header = f.readline()
    except Exception as e:
        print('Failed to read first line of {}: {}'.format(filename, e))
    finally:
        f.close()
    return header

# .............................................................................
def find_chunk_files(big_csv_filename, out_base_filename):
    """Find multiple smaller input csv files from a large input csv file

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records
        out_base_filename (str): Full path and basename (no extension) for output files.

    Returns:
        csv_filename_pairs: a list of filename pairs, containing the input base filename,
            and the output base filename
        chunk_size: the number of records in files, computed from the total records and
            the number of local CPUs for processing
    """
    cpus2use = get_process_count()
    in_base_filename, ext = os.path.splitext(big_csv_filename)

    total_lines = get_line_count(big_csv_filename) - 1
    chunk_size = int(total_lines / cpus2use)

    csv_filename_pairs = []
    start = 1
    stop = chunk_size
    return csv_filename_pairs, chunk_size
    while start <= total_lines:
        in_filename = f"{in_base_filename}_chunk-{start}-{stop}{ext}"
        out_filename =  f"{out_base_filename}_chunk-{start}-{stop}{ext}"
        if os.path.exists(in_filename):
            csv_filename_pairs.append((in_filename, out_filename))
        else:
            # Return basenames if files are not present
            csv_filename_pairs = [(in_base_filename, out_base_filename)]
            print('Missing file {}'.format(in_filename))
            break
        start = stop + 1
        stop = start + chunk_size - 1

# .............................................................................
def get_chunk_files(big_csv_filename, out_base_filename):
    """Split a large input csv file into multiple smaller input csv files.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records
        out_base_filename (str): Full path and basename (no extension) for output files.

    Returns:
        csv_filename_pairs: a list of filename pairs, containing the input base filename,
            and the output base filename
        header: the header for all input and output files.
    """
    csv_filename_pairs, chunk_size = find_chunk_files(big_csv_filename, out_base_filename)

    # First pair is existing files OR basenames
    if os.path.exists(csv_filename_pairs[0][0]):
        header = get_header(big_csv_filename)
    else:
        in_base_filename = csv_filename_pairs[0][0]
        out_base_filename = csv_filename_pairs[0][1]

        csv_filename_pairs = []
        try:
            bigf = open(big_csv_filename, 'r', encoding='utf-8')
            header = bigf.readline()
            line = bigf.readline()
            curr_recno = 1
            while line != '':
                # Reset vars for next chunk
                start = curr_recno
                stop = start + chunk_size - 1
                in_filename = '{}_chunk-{}-{}.csv'.format(in_base_filename, start, stop)
                out_filename =  '{}_chunk-{}-{}.csv'.format(out_base_filename, start, stop)
                csv_filename_pairs.append((in_filename, out_filename))
                try:
                    # Start writing the smaller file
                    inf = open(in_filename, 'w', encoding='utf-8')
                    inf.write('{}'.format(header))
                    while curr_recno <= stop:
                        if line != '':
                            inf.write('{}'.format(line))
                            line = bigf.readline()
                            curr_recno += 1
                        else:
                            curr_recno = stop + 1
                except Exception as inner_err:
                    print('Failed in inner loop {}'.format(inner_err))
                    raise
                finally:
                    inf.close()
        except Exception as outer_err:
            print('Failed to do something {}'.format(outer_err))
            raise
        finally:
            bigf.close()

    return csv_filename_pairs, header

# .............................................................................
def get_line_count(filename):
    """ find total number lines in a file """
    cmd = "wc -l {}".format(repr(filename))
    info, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    temp = info.split(b'\n')[0]
    line_count = int(temp.split()[0])
    return line_count

# .............................................................................
def chunk_files(big_csv_filename):
    """Split a large input csv file into multiple smaller input csv files.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records

    Returns:
        csv_filename_pairs: a list of filename pairs, containing the input base filename,
            and the output base filename
        header: the header for all input and output files.
    """
    cpus2use = cpu_count() - 2
    chunks = []

    in_base_filename, ext = os.path.splitext(big_csv_filename)
    total_lines = get_line_count(big_csv_filename) - 1
    chunk_size = int(total_lines / cpus2use)

    start = 1
    stop = chunk_size

    try:
        bigf = open(big_csv_filename, 'r', encoding='utf-8')
        header = bigf.readline()
        line = bigf.readline()
        curr_recno = 1

        while start <= total_lines and line != '':
            chunk_filename = f"{in_base_filename}_chunk-{start}-{stop}{ext}"
            if os.path.exists(chunk_filename):
                delete_file(chunk_filename)

            try:
                # Start writing the smaller file
                chunkf = open(chunk_filename, 'w', encoding='utf-8')
                chunkf.write('{}'.format(header))
                while curr_recno <= stop:
                    if line != '':
                        chunkf.write(f"{line}")
                        line = bigf.readline()
                        curr_recno += 1
                    else:
                        curr_recno = stop + 1
                chunks.append(chunk_filename)

                # Advance for next chunk
                start = stop + 1
                stop = start + chunk_size - 1

            except Exception as inner_err:
                print(f"Failed in inner loop {inner_err}")
                raise
            finally:
                chunkf.close()

    except Exception as outer_err:
        print(f"Failed to do something {outer_err}")
        raise
    finally:
        bigf.close()

    return chunks

# .............................................................................
if __name__ == "__main__":
    import argparse

    default_infile = os.path.join(DATA_PATH, GBIF.INPUT_DATA)
    default_output_basename = os.path.join(DATA_PATH)

    parser = argparse.ArgumentParser(description="Split")
    parser.add_argument("cmd", type=str, default="split")
    parser.add_argument(
        "big_csv_filename", type=str, default=GBIF.INPUT_DATA,
        help='The full path to GBIF input species occurrence data.')
    args = parser.parse_args()

    if args.cmd != "split":
        print("Only `split` is currently supported")
    else:
        chunk_files(args.big_csv_filename)
    print('Testing, sys path = ', sys.path)
