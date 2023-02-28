"""Common file handling tools used in various BISON modules."""
import csv
import logging
import glob
from logging.handlers import RotatingFileHandler
import math
from multiprocessing import cpu_count
import os
import subprocess
import sys
import time

from bison.common.constants import BIG_DATA_PATH, ENCODING, EXTRA_CSV_FIELD, GBIF, LOG


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
def get_csv_writer(datafile, delimiter, fmode="w", overwrite=True):
    """Create a CSV writer.

    Args:
        datafile: output CSV file for writing
        delimiter: field separator
        fmode: Write ('w') or append ('a')
        overwrite (bool): True to delete an existing file before write

    Returns:
        writer (csv.writer) ready to write
        f (file handle)

    Raises:
        Exception: on failure to create a csv writer
        FileExistsError: on existing file if overwrite is False
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")

    if ready_filename(datafile, overwrite=overwrite):
        csv.field_size_limit(sys.maxsize)
        try:
            f = open(datafile, fmode, newline="", encoding=ENCODING)
            writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        except Exception as e:
            raise e
    else:
        raise FileExistsError

    return writer, f


# .............................................................................
def get_csv_dict_writer(csvfile, header, delimiter, fmode="w", encoding=ENCODING, extrasaction="ignore", overwrite=True):
    """Create a CSV dictionary writer and write the header.

    Args:
        csvfile (str): output CSV filename for writing
        header (list): header for output file
        delimiter (str): field separator
        fmode (str): Write ('w') or append ('a')
        encoding (str): Encoding for output file
        extrasaction (str): Action to take if there are fields in a record dictionary not present in fieldnames
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
            writer = csv.DictWriter(f, fieldnames=header, delimiter=delimiter, extrasaction=extrasaction)
        except Exception as e:
            raise e
        else:
            writer.writeheader()
        return writer, f
    else:
        raise FileExistsError


# .............................................................................
def get_csv_dict_reader(csvfile, delimiter, fieldnames=None, encoding=ENCODING, quote_none=False, restkey=EXTRA_CSV_FIELD):
    """Create a CSV dictionary reader from a file with the first line containing fieldnames.

    Args:
        csvfile (str): output CSV file for reading
        delimiter (char): delimiter between fields
        fieldnames (list): strings with corrected fieldnames, cleaned of illegal characters, for use with records.
        encoding (str): type of encoding
        quote_none (bool): True opens csvfile with QUOTE_NONE, False opens with QUOTE_MINIMAL
        restkey (str): fieldname for extra fields in a record not present in header

    Returns:
        rdr (csv.DictReader): DictReader ready to read
        f (object): open file handle

    Raises:
        FileNotFoundError: on missing csvfile
        PermissionError: on improper permissions on csvfile
    """
    csv.field_size_limit(sys.maxsize)

    if quote_none is True:
        quoting = csv.QUOTE_NONE
    else:
        quoting = csv.QUOTE_MINIMAL

    try:
        #  If csvfile is a file object, it should be opened with newline=""
        f = open(csvfile, "r", newline="", encoding=encoding)
    except FileNotFoundError:
        raise
    except PermissionError:
        raise

    if fieldnames is not None:
        rdr = csv.DictReader(f, fieldnames=fieldnames, quoting=quoting, delimiter=delimiter, restkey=restkey)
    else:
        rdr = csv.DictReader(f, quoting=quoting, delimiter=delimiter, restkey=restkey)

    return rdr, f


# .............................................................................
def get_logger(logpath, logname=None):
    """Get a logger that logs to file and to console in an established format.

    Args:
        logpath (str): path for the logfile
        logname: name for the logger and basename for the logfile.

    Returns:
         a logger
    """
    level = logging.DEBUG
    if logname is not None:
        logfname = os.path.join(logpath, logname + '.log')
    else:
        # get log filename
        logname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logfname = os.path.join(logpath, logname + '_{}.log'.format(timestamp))
    # Delete file if exists; make directory if missing
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
def get_fieldnames(filename, delimiter):
    """Find fieldnames from the first line of a CSV file.

    Args:
         filename (str): Full filename for a CSV file with a header.
         delimiter (str): Delimiter between fields in file records.

    Returns:
         header (list): list of fieldnames in the first line of the file
    """
    header = None
    try:
        f = open(filename, 'r', newline="", encoding='utf-8')
        header = f.readline()
        tmpflds = header.split(delimiter)
        fieldnames = [fld.strip() for fld in tmpflds]
    except Exception as e:
        print('Failed to read first line of {}: {}'.format(filename, e))
    finally:
        f.close()
    return fieldnames


# .............................................................................
def _check_existence(filename_or_pattern):
    is_pattern = True
    # Wildcards?
    try:
        filename_or_pattern.index("*")
    except ValueError:
        try:
            filename_or_pattern.index("?")
        except ValueError:
            is_pattern = False
    if is_pattern:
        files = glob.glob(filename_or_pattern)
        if len(files) == 0:
            raise FileNotFoundError(f"No files match the pattern {filename_or_pattern}")
    elif not os.path.exists(filename_or_pattern):
        raise FileNotFoundError(f"File {filename_or_pattern} does not exist")

    return is_pattern


# .............................................................................
def _parse_wc_output(subproc_output):
    # Return has list of byte-strings, the first contains one or more output lines, the last byte-string is empty.
    # Multiple matching files will produce multiple lines, with total on the last line
    output = subproc_output[0]
    lines = output.split(b"\n")
    # The last line is empty
    lines = lines[:-1]
    line_of_interest = None
    # Find and split line of interest
    if len(lines) == 1:
        line_of_interest = lines[0]
    else:
        for ln in lines:
            try:
                ln.index(b"total")
            except ValueError:
                pass
            else:
                line_of_interest = ln
    if line_of_interest is None:
        raise Exception(f"Failed to get line with results from {subproc_output}")
    elts = line_of_interest.strip().split(b" ")
    # Count is first element in line
    tmp = elts[0]
    try:
        line_count = int(tmp)
    except ValueError:
        raise Exception(f"First element on results line {line_of_interest} is not an integer")
    return line_count


# .............................................................................
def count_lines(filename_or_pattern, grep_strings=None):
    """Find total number of lines in a file.

    Args:
        filename_or_pattern (str): filepath, with or without wildcards, to count lines for
        grep_strings (list): list of strings to find in lines

    Returns:
        line_count (int): number of lines in the file containing all of the strings in str_list

    Raises:
        FileNotFoundError: file pattern matches no files
        FileNotFoundError: file does not exist

    Assumptions:
        Existence of the command line tool "grep".
        Existence of the command line tool "wc"
        Output of "wc" consists of one or more lines with the pattern: <count>  <filename>
            If more than one file is being examined, the last line will have the pattern: <count>  total
    """
    line_count = None
    try:
        _check_existence(filename_or_pattern)
    except FileNotFoundError:
        raise

    # Assemble bash command
    if grep_strings is not None:
        # start with grep command
        st = grep_strings.pop(0)
        cmd = f"grep {st} {filename_or_pattern} | "
        # add additional grep commands
        while len(grep_strings) > 0:
            st = grep_strings.pop(0)
            cmd += f"grep {st} | "
        # count output produced from greps
        cmd += "wc -l"
    else:
        # count all lines
        cmd = f"wc -l {filename_or_pattern}"

    # Run command in a shell
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sp_outs = sp.communicate()

    # Retrieve the total count
    line_count = _parse_wc_output(sp_outs)

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
    if big_csv_filename.endswith(GBIF.INPUT_DATA):
        # shortcut
        rec_count = GBIF.INPUT_RECORD_COUNT
    else:
        rec_count = count_lines(big_csv_filename) - 1
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
def get_chunk_filename(in_base_filename, start, stop, ext):
    """Create a consistent filename for chunks of a larger file.

    Args:
        in_base_filename (str): common base filename for all chunks
        start (int): line number in the large file that serves as the first record of the chunk file
        stop (int): line number in the large file that serves as the last record of the chunk file
        ext (str): file extension for the chunk file

    Returns:
        chunk_filename: standardized filename for the chunk of data
    """
    chunk_filename = f"{in_base_filename}_chunk-{start}-{stop}_raw{ext}"
    return chunk_filename


# .............................................................................
def parse_chunk_filename(chunk_filename):
    """Create a consistent filename for chunks of a larger file.

    Args:
        chunk_filename: standardized base filename for the chunk of data

    Returns:
        in_base_filename (str): common base filename for all chunks
        start (int): line number in the large file that serves as the first record of the chunk file
        stop (int): line number in the large file that serves as the last record of the chunk file
        ext (str): file extension for the chunk file
    """
    idx = chunk_filename.index("_chunk")
    in_base_filename = chunk_filename[:idx]

    basename, ext = os.path.splitext(chunk_filename)
    parts = basename.split("_")
    for i in range(len(parts)):
        if parts[i].startswith("chunk"):
            chunk_idx = i
            break
    _, start, stop = parts[chunk_idx].split("-")

    return in_base_filename, start, stop, ext


# .............................................................................
def identify_chunk_files(big_csv_filename):
    """Construct filenames for smaller files subset from a large file.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records

    Returns:
        chunk_filenames: a list of chunk filenames
    """
    chunk_filenames = []
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    boundary_pairs = identify_chunks(big_csv_filename)
    for (start, stop) in boundary_pairs:
        chunk_fname = get_chunk_filename(in_base_filename, start, stop, ext)
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

    Note:
        Write chunk file records exactly as read, no corrections applied.
    """
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    chunk_filenames = []
    boundary_pairs = identify_chunks(big_csv_filename)

    try:
        bigf = open(big_csv_filename, 'r', newline="", encoding='utf-8')
        header = bigf.readline()
        line = bigf.readline()
        big_recno = 1

        for (start, stop) in boundary_pairs:
            chunk_fname = get_chunk_filename(in_base_filename, start, stop, ext)

            try:
                # Start writing the smaller file
                chunkf = open(chunk_fname, 'w', newline="", encoding='utf-8')
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
                print(f"Wrote lines {start} to {stop} to {chunk_fname}")
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

    parser = argparse.ArgumentParser(description="Split")
    parser.add_argument("cmd", type=str, default="split")
    parser.add_argument(
        "gbif_filename", type=str, default=GBIF.INPUT_DATA,
        help='The full path to GBIF input species occurrence data.')
    args = parser.parse_args()

    # Args may be full path, or base filename in default path
    gbif_filename = args.gbif_filename
    if not os.path.exists(gbif_filename):
        gbif_filename = os.path.join(BIG_DATA_PATH, gbif_filename)

    if args.cmd != "split":
        print("Only `split` is currently supported")
    else:
        boundary_pairs = identify_chunks(args.gbif_filename)
        chunk_filenames = chunk_files(args.gbif_filename)
        print(f"boundary_pairs = {boundary_pairs}")


"""
import csv
import os
import sys

pattern = "/home/astewart/git/bison/data/gbif_2022-03-16_100k_chunk*annotated.csv"
fname = "/home/astewart/git/bison/data/out/riis_summary.csv"

line_count = None
# Check good input file/s
if filename_or_pattern.index("*") > 0 or filename_or_pattern.index("?") > 0:
    files = glob.glob(filename_or_pattern)
    if len(files) == 0:
        raise FileNotFoundError(f"No files match the pattern {filename_or_pattern}")
elif not os.path.exists(filename_or_pattern):
    raise FileNotFoundError(f"File {filename_or_pattern} does not exist")

# Assemble bash command
if strlist is not None:
    # start with grep command
    st = strlist.pop(0)
    cmd = f"grep {st} {filename_or_pattern} | "
    # add additional grep commands
    while len(strlist) > 0:
        st = strlist.pop(0)
        cmd += f"grep {st} | "
    # count lines produced from greps
    cmd += "wc -l"
else:
    # count all lines
    cmd = f"wc -l {filename_or_pattern}"

# Run command in a shell
sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
sp_outs = sp.communicate()
# Return has list of byte strings, which has line count and filename?
for info in sp_outs:
    try:
        results = info.split(b' ')
        tmp = results[0]
        try:
            line_count = int(tmp)
            break
        except ValueError:
            pass
    except Exception as e:
        print(e)
        pass
if line_count is None:
    raise Exception(f"Failed to get line count from {sp_outs}")
return line_count
"""
