"""Common file handling tools used in various BISON modules."""
import csv
import logging
from logging.handlers import RotatingFileHandler
import math
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
        msg = "Cannot delete file 'None'"
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
            f, escapechar="\\", delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
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
        csvfile, delimiter, fieldnames=None, encoding=ENCODING, quote_none=False):
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
    if quote_none:
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
def get_line_count(filename):
    """Find total number of lines in a file.

    Args:
        filename (str): file to count lines

    Returns:
        line_count (int): number of lines in the file

    Raises:
        Exception: on unknown error in line count subprocess
        FileNotFoundError: on missing input file
    """
    line_count = None
    if os.path.exists(filename):
        cmd = f"wc -l {filename}"
        sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sp_outs = sp.communicate()
        # Return has list of byte strings, which has line count and filename?
        for info in sp_outs:
            try:
                results = info.split(b' ')
                tmp = results[0]
                line_count = int(tmp)
                break
            except Exception as e:
                print(e)
                pass
        if line_count is None:
            raise Exception(f"Failed to get line count from {sp_outs}")
    else:
        raise FileNotFoundError(filename)
    return line_count


# .............................................................................
def identify_chunks(big_csv_filename):
    """Determine the start and stop lines in a large file that will make up the contents of smaller subsets of the file.

    The purpose of chunking the files is to split the large file into more manageable chunks that can be processed
     concurrently by the CPUs on the local machine.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records

    Returns:
        start_stop_pairs: a list of tuples, containing pairs of line numbers in the original file that will be the first
            and last record of a subset chunk of the file.
    """
    cpus2use = cpu_count() - 2
    start_stop_pairs = []

    # in_base_filename, ext = os.path.splitext(big_csv_filename)
    rec_count = get_line_count(big_csv_filename) - 1
    chunk_size = math.ceil(rec_count / cpus2use)

    start = 1
    stop = chunk_size
    start_stop_pairs.append((start, stop))

    while stop < rec_count:
        # chunk_filename = f"{in_base_filename}_chunk-{start}-{stop}{ext}"

        # Advance for next chunk
        start = stop + 1
        stop = min((start + chunk_size - 1), rec_count)
        start_stop_pairs.append((start, stop))

    return start_stop_pairs


# .............................................................................
def get_chunk_filename(in_base_filename, start, stop, ext, overwrite=True):
    """Create a consistent filename for chunks of a larger file.

    Args:
        in_base_filename (str): common base filename for all chunks
        start (int): line number in the large file that serves as the first record of the chunk file
        stop (int): line number in the large file that serves as the last record of the chunk file
        ext (str): file extension for the chunk file
        overwrite (bool): flag indicating whether to delete an existing file with the chunk filename.

    Returns:
        chunk_filename: standardized filename for the chunk of data
    """
    chunk_filename = f"{in_base_filename}_chunk-{start}-{stop}{ext}"
    if overwrite is True and os.path.exists(chunk_filename):
        delete_file(chunk_filename)
    return chunk_filename


# .............................................................................
def identify_chunk_files(big_csv_filename):
    chunk_filenames = []
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    boundary_pairs = identify_chunks(big_csv_filename)
    for (start, stop) in boundary_pairs:
        chunk_fname = get_chunk_filename(in_base_filename, start, stop, ext, overwrite=True)
        chunk_filenames.append(chunk_fname)
    return chunk_filenames


# .............................................................................
def chunk_files(big_csv_filename):
    """Split a large input csv file into multiple smaller input csv files.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records

    Returns:
        chunk_filenames: a list of chunk filenames

    Raises:
        Exception: on failure to open or write to a chunk file
        Exception: on failure to open or read the big_csv_filename
    """
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    chunk_filenames = []
    boundary_pairs = identify_chunks(big_csv_filename)

    try:
        bigf = open(big_csv_filename, 'r', encoding='utf-8')
        header = bigf.readline()
        line = bigf.readline()
        big_recno = 1

        for (start, stop) in boundary_pairs:
            chunk_fname = get_chunk_filename(in_base_filename, start, stop, ext, overwrite=True)

            try:
                # Start writing the smaller file
                chunkf = open(chunk_fname, 'w', encoding='utf-8')
                chunkf.write('{}'.format(header))

                while big_recno <= stop and line:
                    try:
                        # Write last line to chunk file
                        chunkf.write(f"{line}")
                    except Exception as e:
                        # Log error and move on
                        print(
                            f"Failed on bigfile {big_csv_filename} line number {big_recno} writing to {chunk_fname}: {e}")
                    # If bigfile still has lines, get next one
                    if line:
                        line = bigf.readline()
                        big_recno += 1
                    else:
                        big_recno = stop + 1

            except Exception as e:
                print(f"Failed opening or writing to {chunk_fname}: {e}")
                raise
            finally:
                # After got to stop, close and add filename to list
                chunkf.close()
                chunk_filenames.append(chunk_fname)

    except Exception as e:
        print(f"Failed to read bigfile {big_csv_filename}: {e}")
        raise
    finally:
        bigf.close()

    return chunk_filenames


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
        boundary_pairs = identify_chunks(args.big_csv_filename)
        chunk_filenames = chunk_files(args.big_csv_filename)
        print(f"boundary_pairs = {boundary_pairs}")
