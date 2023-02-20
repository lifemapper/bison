"""Common file handling tools used in various BISON modules."""
import csv
import glob
import logging
import math
import os
import subprocess
import sys

from bison.common.constants import DWC_PROCESS, ENCODING, EXTRA_CSV_FIELD, GBIF


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
def get_csv_writer(datafile, delimiter, header=None, fmode="w", overwrite=True):
    """Create a CSV writer.

    Args:
        datafile: output CSV file for writing
        delimiter: field separator
        header: list of fieldnames to be written as the first line
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
            if header is not None:
                writer.writerow(header)
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
def get_csv_dict_reader(
        csvfile, delimiter, fieldnames=None, encoding=ENCODING, quote_none=False,
        restkey=EXTRA_CSV_FIELD):
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
def _parse_cat_output(subproc_output):
    # Return has list of byte-strings, the first contains one or more output lines, the last byte-string is empty.
    # Multiple matching files will produce multiple lines, with total on the last line
    output = subproc_output[0]
    lines = output.split(b"\n")
    line_of_interest = lines[0]
    if line_of_interest is None:
        raise Exception(f"Failed to get line with results from {subproc_output}")
    elts = line_of_interest.strip().split(b"\t")
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
def count_lines_with_cat(filename_or_pattern):
    """Find total number of lines in a file.

    Args:
        filename_or_pattern (str): filepath, with or without wildcards, to count lines for

    Returns:
        line_count (int): number of lines in the file

    Raises:
        FileNotFoundError: file pattern matches no files
        FileNotFoundError: file does not exist

    Assumptions:
        Existence of the command line tool "cat".
        Existence of the command line tool "tail"
    """
    line_count = None
    try:
        _check_existence(filename_or_pattern)
    except FileNotFoundError:
        raise
    cmd = f"cat -n {filename_or_pattern} | tail -n1"

    # Run command in a shell
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sp_outs = sp.communicate()

    # Retrieve the total count
    line_count = _parse_cat_output(sp_outs)

    return line_count


# .............................................................................
def identify_chunks(big_csv_filename, chunk_count=0):
    """Determine the start and stop lines in a large file that will make up the contents of smaller subsets of the file.

    The purpose of chunking the files is to split the large file into more manageable chunks that can be processed
     concurrently by the CPUs on the local machine.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records
        chunk_count (int): Number of smaller files to split large file into.  Defaults
            to the number of available CPUs minus 2.

    Returns:
        start_stop_pairs: a list of tuples, containing pairs of line numbers in the original file that will be the first
            and last record of a subset chunk of the file.
    """
    if chunk_count == 0:
        chunk_count = available_cpu_count() - 2
    start_stop_pairs = []

    # in_base_filename, ext = os.path.splitext(big_csv_filename)
    if big_csv_filename.endswith(GBIF.INPUT_DATA):
        # shortcut
        rec_count = GBIF.INPUT_RECORD_COUNT
    else:
        rec_count = count_lines(big_csv_filename) - 1
    chunk_size = math.ceil(rec_count / chunk_count)

    start = 1
    stop = chunk_size
    start_stop_pairs.append((start, stop))

    while stop < rec_count:
        # chunk_filename = f"{in_base_filename}_chunk-{start}-{stop}{ext}"

        # Advance for next chunk
        start = stop + 1
        stop = min((start + chunk_size - 1), rec_count)
        start_stop_pairs.append((start, stop))

    return start_stop_pairs, rec_count, chunk_size


# .............................................................................
def get_fields_from_header(csvfile, delimiter=GBIF.DWCA_DELIMITER, encoding="utf-8"):
    """Find fields in a header in a delimited text file.

    Args:
        csvfile (str): comma/tab-delimited file with header
        delimiter (str): single character delimiter between fields
        encoding (str): encoding of the file

    Returns:
        list: of strings indicating fieldnames

    Raises:
        FileNotFoundError: file does not exist
        Exception: unknown read error
    """
    fields = []
    try:
        _check_existence(csvfile)
    except FileNotFoundError:
        raise

    # Open file and read first line
    try:
        f = open(csvfile, "r", newline="", encoding=encoding)
        line = f.readline()
        line = line.strip()
        fields = line.split(delimiter)
    except Exception:
        raise
    finally:
        f.close()

    return fields


# .............................................................................
def identify_chunk_files(big_csv_filename, chunk_count=0):
    """Construct filenames for smaller files subset from a large file.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records
        chunk_count (int): Number of smaller files to split large file into.  Defaults
            to the number of available CPUs minus 2.

    Returns:
        chunk_filenames: a list of chunk filenames
    """
    chunk_filenames = []
    in_base_filename, ext = os.path.splitext(big_csv_filename)
    boundary_pairs, _rec_count, _chunk_size = identify_chunks(
        big_csv_filename, chunk_count=chunk_count)
    for (start, stop) in boundary_pairs:
        chunk_fname = BisonNameOp.get_chunk_filename(in_base_filename, ext, start, stop)
        chunk_filenames.append(chunk_fname)
    return chunk_filenames


# .............................................................................
def chunk_files(big_csv_filename, output_path, logger, chunk_count=0):
    """Split a large input csv file into multiple smaller input csv files.

    Args:
        big_csv_filename (str): Full path to the original large CSV file of records
        output_path (str): Destination directory for chunked files.
        logger (object): logger for writing messages to file and console
        chunk_count (int): Number of smaller files to split large file into.  Defaults
            to the number of available CPUs minus 2.

    Returns:
        chunk_filenames: a list of chunk filenames

    Raises:
        Exception: on failure to open or write to a chunk file
        Exception: on failure to open or read the big_csv_filename

    Note:
        Write chunk file records exactly as read, no corrections applied.
    """
    refname = "chunk_files"
    inpath, base_filename = os.path.split(big_csv_filename)
    basename, ext = os.path.splitext(base_filename)
    chunk_filenames = []
    boundary_pairs, rec_count, chunk_size = identify_chunks(
        big_csv_filename, chunk_count=chunk_count)

    try:
        bigf = open(big_csv_filename, 'r', newline="", encoding='utf-8')
        header = bigf.readline()
        line = bigf.readline()
        big_recno = 1

        for (start, stop) in boundary_pairs:
            chunk_basefilename = BisonNameOp.get_chunk_filename(
                basename, ext, start, stop)
            chunk_fname = os.path.join(output_path, chunk_basefilename)

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
                        logger.log(
                            f"Failed on bigfile {big_csv_filename} line number "
                            f"{big_recno} writing to {chunk_fname}: {e}",
                            refname=refname, log_level=logging.ERROR)
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
                logger.log(
                    f"Wrote lines {start} to {stop} to {chunk_fname}", refname=refname)
                chunk_filenames.append(chunk_fname)

    except Exception as e:
        logger.log(
            f"Failed to read bigfile {big_csv_filename}: {e}", refname=refname,
            log_level=logging.ERROR)
        raise
    finally:
        bigf.close()
    report = {
        "large_filename": big_csv_filename,
        "chunked_files": chunk_filenames,
        "record_count": rec_count,
        "chunk_size": chunk_size
    }

    return chunk_filenames, report


# .............................................................................
class BisonNameOp():

    @staticmethod
    def get_chunk_filename(basename, ext, start, stop):
        """Construct a filename for a chunk of CSV records.

        Args:
            basename (str): base filename of the original large CSV data.
            ext (str): extension of the filename
            start (int): record number in original file of first record for data chunk.
            stop (int): record number in original file of last record for data chunk.

        Returns:
            str: base filename for the subset file.

        Note:
            File will always start with basename,
            followed by chunk
            followed by process step completed (if any)
        """
        postfix = f"{DWC_PROCESS.CHUNK['prefix']}-{start}-{stop}"
        return f"{basename}{DWC_PROCESS.SEP}{postfix}{ext}"

    # .............................................................................
    @staticmethod
    def get_out_filename(in_filename, outpath=None):
        """Construct output filename for the next processing step of the given file.

        Args:
            in_filename (str): base or full filename of CSV data.
            outpath (str): destination directory for output filename

        Returns:
            out_fname: base or full filename of output file, given the input filename.
                If the input filename reflects the final processing step, the method
                returns None

        Note:
            The input filename is parsed for process step, and the output filename will
            be constructed for the next step.

            File will always start with basename, followed by chunk,
                followed by process step completed (if any)
        """
        outfname = None
        path, basename, ext, chunk, postfix = BisonNameOp.parse_process_filename(
            in_filename)
        if chunk is not None:
            basename = f"{basename}{DWC_PROCESS.SEP}{chunk}"
        this_step = DWC_PROCESS.get_step(postfix)
        next_postfix = DWC_PROCESS.get_postfix(this_step + 1)
        if next_postfix is not None:
            outbasename = f"{basename}{DWC_PROCESS.SEP}{next_postfix}{ext}"
            if outpath is None:
                outpath = path
            outfname = os.path.join(outpath, outbasename)
        return outfname

    # .............................................................................
    @staticmethod
    def parse_process_filename(filename):
        """Parse a filename into path, basename, chunk, processing step, extension.

        Args:
            filename (str): A filename used in processing

        Returns:
            path: file path of the filename, if included
            basename: basename of the filename
            ext: extension of the filename
            chunk: the chunk string, chunk-<start>-<stop>, where start and stop indicate
                the record (line+1) numbers in the original datafile.
            process_postfix: the postfix of the file, indicating which stage of
                processing has been completed.

        Note:
            File will always start with basename,
            followed by chunk (if chunked)
            followed by process step completed (if any)
        """
        chunk = None
        process_postfix = None
        # path will be None if filename is basefilename
        path, fname = os.path.split(filename)
        basefname, ext = os.path.splitext(fname)
        parts = basefname.split(DWC_PROCESS.SEP)
        # File will always start with basename
        basename = parts.pop(0)
        if len(parts) >= 1:
            p = parts.pop(0)
            # if chunk exists
            if not p.startswith(DWC_PROCESS.CHUNK["prefix"]):
                process_postfix = p
            else:
                chunk = p
                if len(parts) >= 1:
                    process_postfix = parts.pop(0)
        return path, basename, ext, chunk, process_postfix

    # ...............................................
    @staticmethod
    def construct_location_summary_name(outpath, region, prefix):
        """Construct a filename for the summary file for a region.

        Args:
            outpath (str): full directory path for computations and output.
            region (str): region name
            prefix (str): file prefix indicating region type

        Returns:
            outfname: output filename derived from the state and county
        """
        basename = f"{prefix}_{region}.csv"
        outfname = os.path.join(outpath, basename)
        return outfname

    # ...............................................
    @staticmethod
    def construct_assessment_summary_name(outpath):
        """Construct a filename for the RIIS assessment summary file.

        Args:
            outpath (str): full directory path for computations and output.

        Returns:
            outfname: output filename
        """
        outfname = os.path.join(outpath, "riis_summary.csv")
        return outfname

    # ...............................................
    @staticmethod
    def parse_location_summary_name(csvfile):
        """Construct a filename for the summarized version of csvfile.

        Args:
            csvfile (str): full filename used to construct an annotated filename
                for this data.

        Returns:
            outfname: output filename derived from the annotated GBIF DWC filename

        Raises:
            Exception: on filename does not start with "state_" or "county_"
        """
        county = None
        _, basefilename = os.path.split(csvfile)
        basename, ext = os.path.splitext(basefilename)
        if basename.startswith("state_"):
            _, state = basename.split("_")
        elif basename.startswith("county_"):
            _, state, county = basename.split("_")
        else:
            raise Exception(
                f"Filename {csvfile} cannot be parsed into location elements")
        return state, county


# .............................................................................
def available_cpu_count():
    """Number of available virtual or physical CPUs on this system.

    Returns:
        int for the number of CPUs available

    Raises:
        Exception: on failure of all CPU count queries.

    Notes:
        code from https://stackoverflow.com/questions/1006289
    """
    # Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # https://github.com/giampaolo/psutil
    try:
        import psutil
        return psutil.cpu_count()   # psutil.NUM_CPUS on old versions
    except (ImportError, AttributeError):
        pass

    # POSIX
    try:
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))
        if res > 0:
            return res
    except (AttributeError, ValueError):
        pass

    # Windows
    try:
        res = int(os.environ['NUMBER_OF_PROCESSORS'])
        if res > 0:
            return res
    except (KeyError, ValueError):
        pass

    # Linux
    try:
        res = open('/proc/cpuinfo').read().count('processor\t:')
        if res > 0:
            return res
    except IOError:
        pass

    raise Exception('Can not determine number of CPUs on this system')


# .............................................................................
__all__ = [
    "available_cpu_count",
    "chunk_files",
    "count_lines",
    "delete_file",
    "get_csv_dict_reader",
    "get_csv_dict_writer",
    "get_csv_writer",
    "get_fieldnames",
    "identify_chunk_files",
    "identify_chunks",
    "ready_filename"
]
