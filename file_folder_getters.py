from time import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from filecmp import cmp as compare_files
from progress_bar import progress_bar
import hashlib


def get_all_files_in_folder(path) -> tuple[str]:
    """
    returns a tuple of all the files in a folder and subfolders,
    the strings in the tuple are full absolute file paths
    """
    assert (os.path.exists(path)), "path does not exist"

    files = list()

    for path_to_file, _, sub_files in os.walk(os.path.abspath(path)):
        files.extend([os.path.abspath(path_to_file+"/"+sub_file) for sub_file in sub_files])

    return tuple(files)


def get_file_extensions_singlethreaded(filepaths: tuple[str]) -> tuple[str]:
    """
    returns a tuple of all unique file extensions in the filepaths given
    """
    assert (isinstance(filepaths, tuple)), "filepaths was not a tuple"

    file_extensions = set()

    for filepath in filepaths:
        assert (isinstance(filepath, str)), "at least one filepath was not a string"
        filepath = os.path.basename(filepath) # get only the filename (to exclude folders with "." with files with no extension)
        file_extension = "."+filepath.split(".")[-1]
        if (not filepath.startswith(".")) and (not file_extension.count(" ")): # makes sure files don't start with "." or contain a space after "."
            file_extensions.add(file_extension)

    return tuple(file_extensions)


def get_file_extensions(filepaths: tuple[str], files_per_group: int = 100000) -> tuple[str]:
    """
    returns a tuple of all unique file extensions in the filepaths given
    """
    assert (isinstance(filepaths, tuple)), "filepaths was not a tuple"

    file_extensions = set()

    grouped_filepaths = [filepaths[i:i+files_per_group] if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

    threads = list()

    with ProcessPoolExecutor() as executor:
        for filepaths in grouped_filepaths:
            thread = executor.submit(get_file_extensions_singlethreaded, filepaths)
            threads.append(thread)
        wait(threads)
        [file_extensions.update(set(thread.result())) for thread in threads]

    return tuple(file_extensions)


def limit_files_by_file_extension(filepaths: tuple[str], file_extensions: tuple[str]) -> tuple[str]:
    """
    returns a limited version of the input filepaths tuple, by only keeping
    any files that have a file extension in the file_extensions tuple
    """
    assert (isinstance(filepaths, tuple)), "filepaths was not a tuple"
    assert (isinstance(file_extensions, tuple)), "file_extensions was not a tuple"

    new_filepaths: tuple[str] = tuple([filepath for filepath in filepaths if filepath.endswith(file_extensions)])

    return new_filepaths


def limit_files_by_file_start(filepaths: tuple[str], file_starts: tuple[str]) -> tuple[str]:
    """
    returns a limited version of the input filepaths tuple, by only keeping
    any files that their filenames start with one of the strings in file_starts
    """
    assert (isinstance(filepaths, tuple)), "filepaths was not a tuple"
    assert (isinstance(file_starts, tuple)), "file_starts was not a tuple"

    new_filepaths: list[str] = list()

    for filepath in filepaths:
        filename = os.path.basename(filepath)
        if filename.startswith(file_starts):
            new_filepaths.append(filename)

    return tuple(new_filepaths)


def get_small_or_large_files(filepaths: tuple[str], size_cutoff: int, is_max: bool = True) -> tuple[tuple[str, int]]: # FIXME not particularly useful anymore? it's just a bit odd
    """
    gets all files from path which:
        are less than or equal to the size cutoff if is_max is True
        are greater than or equal to the size cutoff if is_max is False
    
    returns a tuple of tuples of the full filepaths and their corresponding filesize
    """
    assert (isinstance(filepaths, tuple)), "path does not exist"
    assert (isinstance(size_cutoff, int)), "size cutoff was not an int"
    assert (size_cutoff >= 0), "size cutoff was negative"
    assert (isinstance(is_max, bool)), "is_max was not a bool"

    files_sizes_pairs: list[tuple[str, int]] = list()

    for filepath in filepaths:
        try: # faster than checking if file exists
            file_size = os.stat(filepath).st_size
        except:
            continue # skip filepath
        if (is_max and file_size <= size_cutoff) or (not is_max and file_size >= size_cutoff):
            files_sizes_pairs.append((filepath, file_size))

    return tuple(files_sizes_pairs)


def limit_files_by_size_singlethreaded(filepaths: tuple[str], min_size: int = 0, max_size: int = 2**64) -> tuple[str]:
    """
    limits files to only keep files between min_size and max_size
    min and maxes are inclusive
    """
    assert (isinstance(filepaths, tuple)), "path does not exist"
    assert (isinstance(min_size, int)), "min_size was not an int"
    assert (isinstance(max_size, int)), "max_size was not an int"
    assert (max_size >= min_size), "max_size must be greater than min_size"

    new_filepaths: list[str] = list()

    for filepath in filepaths:
        try: # faster than checking if file exists
            file_size = os.stat(filepath).st_size
        except:
            continue # skip filepath
        if file_size >= min_size and file_size <= max_size:
            new_filepaths.append(filepath)

    return tuple(new_filepaths)


def limit_files_by_size(filepaths: tuple[str], min_size: int = 0, max_size: int = 2**64, files_per_group: int = 100) -> tuple[str]:
    """
    limits files to only keep files between min_size and max_size
    min and maxes are inclusive
    """
    assert (isinstance(filepaths, tuple)), "path does not exist"
    assert (isinstance(min_size, int)), "min_size was not an int"
    assert (isinstance(max_size, int)), "max_size was not an int"
    assert (max_size >= min_size), "max_size must be greater than min_size"

    new_filepaths: list[str] = list()

    grouped_filepaths = [filepaths[i:i+files_per_group] if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

    threads = list()

    with ThreadPoolExecutor() as executor:
        for filepaths in grouped_filepaths:
            thread = executor.submit(limit_files_by_size_singlethreaded, filepaths)
            threads.append(thread)
        wait(threads)
        [new_filepaths.extend(thread.result()) for thread in threads]

    return tuple(new_filepaths)


def get_duplicate_files(filepaths1: tuple[str], filepaths2: tuple[str]) -> tuple[tuple[str, str]]: # TODO multithread
    """
    returns a tuple of all the files that are duplicated between path1 and path2,
    as a tuple of the full path of the first instance, and the full path of the second instance.

    if path1 and path2 are the same, will ignore case when filenames match, of course.

    does not return files that have 0 bytes size, although all such files would match with each other.
    """
    assert (isinstance(filepaths1, tuple)), "path1 does not exist"
    assert (isinstance(filepaths2, tuple)), "path2 does not exist"

    paths_are_identical = (set(filepaths1) == set(filepaths2))
    duplicate_files: set[tuple[str, str]] = set()
    file_paths_by_size: dict[int, tuple[list[tuple[str, str]]]] = dict()
    # keys are size, values are tuples of size 2, first files from path1 then files from path2,
    # inside that tuple is a list of tuples containing
    # the full filepaths of any files of this size, and their sha256 hashes

    print("counting files in folders...")

    files_in_path1 = len(filepaths1)
    if not paths_are_identical:
        files_in_path2 = len(filepaths2)

    print("getting filesizes and hashes...")

    progress = progress_bar(100, rate_units="files")
    file_counter = 0

    for filepath in filepaths1:
        file_counter += 1
        try: # faster than checking if file exists
            file_size = os.stat(filepath).st_size
        except:
            continue # skipe filepath
        if file_size == 0:
            continue # all files of 0 bytes would match, which is very slow and unnecessary
        file_hash = get_hash(filepath, buffer_chunk_size=1048576, only_read_one_chunk=True)
        try:
            file_paths_by_size[file_size][0].append((filepath, file_hash))
        except KeyError: # can't append if the list hadn't been created
            file_paths_by_size[file_size] = ([(filepath, file_hash)], list())
        progress.print_progress_bar(file_counter / files_in_path1, file_counter)

    print("") # to add a newline after the end of the progress bar

    if not paths_are_identical:
        progress = progress_bar(100, rate_units="files")
        file_counter = 0
        for filepath in filepaths2:
            file_counter += 1
            try:
                file_size = os.stat(filepath).st_size
            except:
                continue # skipe filepath
            if file_size == 0:
                continue # all files of 0 bytes would match, which is very slow and unnecessary
            file_hash = get_hash(filepath, buffer_chunk_size=1048576, only_read_one_chunk=True)
            try:
                file_paths_by_size[file_size][1].append((filepath, file_hash))
            except KeyError: # can't append if the list hadn't been created
                file_paths_by_size[file_size] = (list(), [(filepath, file_hash)])
            progress.print_progress_bar(file_counter / files_in_path2, file_counter)

    print("") # to add a newline after the end of the progress bar

    progress = progress_bar(100, rate_units="file-comparisons")

    total_comparisons = 0
    if paths_are_identical:
        for key in file_paths_by_size.keys():
            total_comparisons += len(file_paths_by_size[key][0]) ** 2
    else:
        for key in file_paths_by_size.keys():
            total_comparisons += len(file_paths_by_size[key][0]) * len(file_paths_by_size[key][1])

    current_comparison = 0

    print("finding duplicates...")

    for key in file_paths_by_size.keys():
        if paths_are_identical:
            # then duplicates are only in the first element of the tuple
            potential_duplicates: tuple[list[tuple[str, str]], list[tuple[str, str]]] = (file_paths_by_size[key][0], file_paths_by_size[key][0])
        else:
            potential_duplicates: tuple[list[tuple[str, str]], list[tuple[str, str]]] = (file_paths_by_size[key][0], file_paths_by_size[key][1])
        for file_hash_pair1 in potential_duplicates[0]:
            file1 = file_hash_pair1[0]
            hash1 = file_hash_pair1[1]
            file1_extension = file1.split(".")[-1]
            files_to_compare = [file_hash_pair2[0] for file_hash_pair2 in potential_duplicates[1]
                                    if (file_hash_pair2[0].split(".")[-1] == file1_extension
                                        and file1 != file_hash_pair2[0]
                                        and hash1 == file_hash_pair2[1])]
            current_comparison += len(files_to_compare)
            total_comparisons -= (len(potential_duplicates[1]) - len(files_to_compare)) # number of files easily skipped
            total_comparisons = max(total_comparisons, 0) # in case somehow it goes below 0
            # only compare files byte-by-byte if they have the same filetype, aren't the same path, and have the same hash
            for file2 in files_to_compare:
                pair_already_in_duplicate_files = (duplicate_files.issuperset((file1, file2)) or duplicate_files.issuperset((file2, file1)))
                if pair_already_in_duplicate_files:
                    continue # skip, as we have already found this duplicate
                try:
                    files_are_identical = compare_files(file1, file2, shallow=False)
                except:
                    files_are_identical = False
                # verify that files are actually identical all the way through, byte for byte, since
                # the hash only read the first little bit of each file
                if files_are_identical:
                    duplicate_files.add((file1, file2))

            progress.print_progress_bar(current_comparison/total_comparisons, current_comparison)

    print("") # to add a newline after the end of the progress bar

    return tuple(duplicate_files)


def get_hash(file, buffer_chunk_size: int = 16777216, only_read_one_chunk: bool = False) -> str:
    """
    gets the hash (sha256) of a file
    default buffer size of 16MiB
    """
    assert (os.path.exists(file)), "file doesn't exist"
    assert (isinstance(buffer_chunk_size, int)), "buffer chunk size needs to be an int"
    assert (buffer_chunk_size > 0), "buffer chunk size was too small"

    sha256 = hashlib.sha256()

    try:
        with open(file, 'rb') as f:
            while True:
                chunk = f.read(buffer_chunk_size)
                if not chunk: # once whole file has been read
                    break
                sha256.update(chunk)
                if only_read_one_chunk:
                    break
    except:
        return ""

    return sha256.hexdigest()


def get_size_of_files_singlethreaded(filepaths: tuple[str]) -> int: # FIXME will be replaced/updated when I implement getting os.stat of all files in advance
    """
    gets the sum of all file sizes
    """
    assert (isinstance(filepaths, tuple))

    total_size = 0
    for filepath in filepaths:
        assert (isinstance(filepath, str))
        try:
            total_size += os.stat(filepath).st_size # bytes filesize
        except OSError: # tried to access a restricted file
            pass

    return total_size


def get_size_of_files(filepaths: tuple[str], files_per_group: int = 100) -> int:
    """
    gets the sum of all file sizes

    seems about the same speed as the singlethreaded version
    """
    assert (isinstance(filepaths, tuple))

    grouped_filepaths = [filepaths[i:i+files_per_group] if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

    threads = list()
    total_size = 0
    with ThreadPoolExecutor() as executor:
        for filepaths in grouped_filepaths:
            thread = executor.submit(get_size_of_files_singlethreaded, filepaths)
            threads.append(thread)
        wait(threads)
        total_size += sum(thread.result() for thread in threads)

    return total_size


if __name__ == "__main__":
    files = get_all_files_in_folder("C:/")
    start_time = time()
    print(len(files))
    #size_of_folder = get_size_of_files_multithreaded(files)
    #print(size_of_folder)
    file_extensions = get_file_extensions(files)
    print(len(file_extensions))
    #limited_files = limit_files_by_size(files)
    #print(len(limited_files))
    """
    print(len(files))
    print("got files in {} seconds".format(time() - start_time))
    new_time = time()
    files = limit_files_by_file_extension(files, (".txt",))
    print(len(files))
    print(files[:20])
    print("limited by file extension in {} seconds".format(time() - new_time))
    new_time = time()
    files = limit_files_by_size(files, 1024**2, is_max=False)
    print(len(files))
    print(files[:20])
    print("limited by filesize in {} seconds".format(time() - new_time))
    new_time = time()
    files = limit_files_by_file_start(files, ("a",))
    print(len(files))
    print(files)
    print("limited by file start in {} seconds".format(time() - new_time))
    """

    """
    duplicates = get_duplicate_files(files, files)

    import csv
    with open("duplicate_files.csv", "w", newline="") as file:
        csv_writer = csv.writer(file)
        for duplicate_pair in duplicates:
            try:
                csv_writer.writerow(duplicate_pair)
            except UnicodeEncodeError:
                pass
    """
    print("{} seconds".format(time() - start_time))
