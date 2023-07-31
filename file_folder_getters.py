from time import time, sleep
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
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


def is_folder_empty(path) -> bool:
    """
    returns True if the folder has no files (it can have subfolders)
    """
    files = 0
    for _, _, sub_files in os.walk(os.path.abspath(path)):
        files += len(sub_files)
        if files:
            return False

    return True


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

    grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

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

    grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

    threads = list()

    with ThreadPoolExecutor() as executor:
        for filepaths in grouped_filepaths:
            thread = executor.submit(limit_files_by_size_singlethreaded, filepaths, min_size, max_size)
            threads.append(thread)
        wait(threads)
        [new_filepaths.extend(thread.result()) for thread in threads]

    return tuple(new_filepaths)


def get_duplicate_files(filepaths1: tuple[str], filepaths2: tuple[str], files_per_group: int = 100) -> tuple[tuple[tuple[str], tuple[str]]]:
    """
    returns all the files that are duplicated between path1 and path2,
    as a tuple (each unique file/match)
    of tuples (path1 on the left, path2 on the right)
    of tuples (each file in that path that matches with the others) 
    of strings (the filepaths)

    if path1 and path2 are the same, will ignore case when filenames match, of course.

    does not return files that have 0 bytes size, although all such files would match with each other.

    the greater the total size of duplicates in the filepaths, the longer this will take, as entire files
    will be read to verify that files are in fact duplicates.
    """
    assert (isinstance(filepaths1, tuple)), "path1 does not exist"
    assert (isinstance(filepaths2, tuple)), "path2 does not exist"

    filepaths1_set = set(filepaths1)
    filepaths2_set = set(filepaths2)
    paths_are_identical = (filepaths1_set == filepaths2_set)
    filepaths_grouped_by_size: dict[int, tuple[list[str]]] = dict()
    filepath_sizes: dict[str, int] = dict() # a way to quickly get filesize of any file once we have found them all
    # keys are size, values are tuples of size 2, first files from path1 then files from path2,
    # inside that tuple is a list of tuples containing
    # the full filepaths of any files of this size, and their sha256 hashes
    filepaths_grouped_by_size_hash1: dict[tuple[int, str], tuple[list[str]]] = dict() # group by size and hash of first chunk of each file
    filepath_hash1s: dict[str, str] = dict() # a way to quickly get the first chunk hash of any file once we have found them all
    filepaths_grouped_by_size_hash2: dict[tuple[int, str, str], tuple[list[str]]] = dict() # group by size and hash of first chunk and hash of entire file

    print("counting files in folders...")

    files_to_process = len(filepaths1)
    filepathss = (filepaths1,)
    if not paths_are_identical:
        files_to_process += len(filepaths2)
        filepathss = (filepaths1, filepaths2)

    print("{} files to process".format(files_to_process))
    #####################################################################################################################
    print("getting filesizes...")
    print("creating threads...")

    progress = progress_bar(100, rate_units="threads")
    progress2 = progress_bar(100, rate_units="threads") # threads start running once the first ones are created, so start time here
    thread_counter = 0
    file_counter = 0
    size_threads = list()
    ordered_filepaths = list()

    with ThreadPoolExecutor() as executor:
        for filepaths in filepathss:
            grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]
            for filepaths_group in grouped_filepaths:
                thread_counter += 1
                file_counter += len(filepaths_group)
                thread = executor.submit(__get_multiple_file_sizes, filepaths_group)
                size_threads.append(thread)
                ordered_filepaths.extend(filepaths_group)
                progress.print_progress_bar(file_counter / files_to_process, thread_counter)

        print("") # to add a newline afer the end of the progress bar

        print("waiting for {} threads to return...".format(len(size_threads)))

        all_threads_done = False
        last_done_count = 0
        while not all_threads_done:
            done_count = 0
            for thread in size_threads:
                done_count += thread.done()
            all_threads_done = (done_count == len(size_threads))
            if last_done_count != done_count:
                last_done_count = done_count
                progress2.print_progress_bar(done_count / len(size_threads), done_count)
            else:
                sleep(min((progress2.get_ETA(done_count / len(size_threads))/100, 1)))

        print("") # to add a newline after the end of the progress bar

        print("processing filesizes...")

        file_size_groups: list[tuple[int]] = [thread.result() for thread in size_threads]

    ordered_file_sizes: list[int] = list()
    [ordered_file_sizes.extend(file_size_group) for file_size_group in file_size_groups]
    progress = progress_bar(100, rate_units="files")
    i = -1

    for filepath in ordered_filepaths:
        i += 1
        first_filepaths = (filepath in filepaths1_set)
        if first_filepaths:
            index = 0
        else:
            index = 1
        file_size = ordered_file_sizes[i]
        filepath_sizes[filepath] = file_size
        if file_size == 0:
            continue # all files with 0 size would match which is unnecessarily slow
        try:
            filepaths_grouped_by_size[file_size][index].append(filepath)
        except KeyError: # can't append if the list hadn't been created
            if first_filepaths:
                filepaths_grouped_by_size[file_size] = ([filepath], list())
            else:
                filepaths_grouped_by_size[file_size] = (list(), [filepath])
        if i % files_per_group == 0:
            progress.print_progress_bar((i+1) / files_to_process, i+1)
    progress.print_progress_bar(1, i+1)

    print("") # to add a newline after the end of the progress bar

    print("counting size matches...")

    size_match_filepathss: tuple[list[str]] = (list(), list()) # same format as filepathss

    for filesize in filepaths_grouped_by_size.keys():
        if ((not paths_are_identical and (len(filepaths_grouped_by_size[filesize][0]) > 0 and len(filepaths_grouped_by_size[filesize][1]) > 0)) or
           (paths_are_identical and len(filepaths_grouped_by_size[filesize][0]) > 1)):
            # then there are potential matches
            size_match_filepathss[0].extend(filepaths_grouped_by_size[filesize][0])
            size_match_filepathss[1].extend(filepaths_grouped_by_size[filesize][1])
    
    files_to_process = len(size_match_filepathss[0]) + len(size_match_filepathss[1])

    print("{} files remaining to process".format(files_to_process))
    #####################################################################################################################
    print("getting first MB hashes...")
    print("creating threads...")

    progress = progress_bar(100, rate_units="threads")
    progress2 = progress_bar(100, rate_units="threads") # threads start running once the first ones are created, so start time here
    thread_counter = 0
    file_counter = 0
    hash_threads = list()
    ordered_filepaths = list()

    with ProcessPoolExecutor() as executor:
        for filepaths in size_match_filepathss: # only get hashes of potential matches
            grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]
            for filepaths_group in grouped_filepaths:
                thread_counter += 1
                file_counter += len(filepaths_group)
                thread = executor.submit(__get_multiple_file_hashes, filepaths_group, buffer_chunk_size=1048576, only_read_one_chunk=True)
                hash_threads.append(thread)
                ordered_filepaths.extend(filepaths_group)
                progress.print_progress_bar(file_counter / files_to_process, thread_counter)

        print("") # to add a newline afer the end of the progress bar

        print("waiting for {} threads to return...".format(len(hash_threads)))

        all_threads_done = False
        last_done_count = 0
        while not all_threads_done:
            done_count = 0
            for thread in hash_threads:
                done_count += thread.done()
            all_threads_done = (done_count == len(hash_threads))
            if last_done_count != done_count:
                last_done_count = done_count
                progress2.print_progress_bar(done_count / len(hash_threads), done_count)
            else:
                sleep(min((progress2.get_ETA(done_count / len(hash_threads))/100, 1)))

        print("") # to add a newline after the end of the progress bar

        print("processing hashes...")

        file_hash_groups: list[tuple[str]] = [thread.result() for thread in hash_threads]

    file_hashes: list[str] = list()
    [file_hashes.extend(file_hash_group) for file_hash_group in file_hash_groups]
    progress = progress_bar(100, rate_units="files")
    i = -1

    for filepath in ordered_filepaths:
        i += 1
        first_filepaths = (filepath in filepaths1_set)
        if first_filepaths:
            index = 0
        else:
            index = 1
        file_size = filepath_sizes[filepath]
        file_hash = file_hashes[i]
        filepath_hash1s[filepath] = file_hash
        if file_hash == "":
            continue # happens if there was an error in getting the hash
        try:
            filepaths_grouped_by_size_hash1[(file_size, file_hash)][index].append(filepath)
        except KeyError: # can't append if the list hadn't been created
            if first_filepaths:
                filepaths_grouped_by_size_hash1[(file_size, file_hash)] = ([filepath], list())
            else:
                filepaths_grouped_by_size_hash1[(file_size, file_hash)] = (list(), [filepath])
        if i % files_per_group == 0:
            progress.print_progress_bar((i+1) / files_to_process, i+1)
    progress.print_progress_bar(1, i+1)

    print("") # to add a newline after the end of the progress bar

    print("counting first MB hash matches...")

    hash1_match_filepathss: tuple[list[str]] = (list(), list()) # same format as filepathss

    for filehash1 in filepaths_grouped_by_size_hash1.keys():
        if ((not paths_are_identical and (len(filepaths_grouped_by_size_hash1[filehash1][0]) > 0 and len(filepaths_grouped_by_size_hash1[filehash1][1]) > 0)) or
           (paths_are_identical and len(filepaths_grouped_by_size_hash1[filehash1][0]) > 1)):
            # then there are potential matches
            hash1_match_filepathss[0].extend(filepaths_grouped_by_size_hash1[filehash1][0])
            hash1_match_filepathss[1].extend(filepaths_grouped_by_size_hash1[filehash1][1])
    
    files_to_process = len(hash1_match_filepathss[0]) + len(hash1_match_filepathss[1])

    print("{} files remaining to process".format(files_to_process))
    #####################################################################################################################
    print("getting whole file hashes...")
    print("creating threads...")

    progress = progress_bar(100, rate_units="threads")
    progress2 = progress_bar(100, rate_units="threads") # threads start running once the first ones are created, so start time here
    thread_counter = 0
    file_counter = 0
    hash_threads = list()
    ordered_filepaths = list()

    with ProcessPoolExecutor() as executor:
        for filepaths in hash1_match_filepathss: # only get hashes of potential matches
            grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]
            for filepaths_group in grouped_filepaths:
                thread_counter += 1
                file_counter += len(filepaths_group)
                thread = executor.submit(__get_multiple_file_hashes, filepaths_group, buffer_chunk_size=1048576, only_read_one_chunk=False)
                hash_threads.append(thread)
                ordered_filepaths.extend(filepaths_group)
                progress.print_progress_bar(file_counter / files_to_process, thread_counter)

        print("") # to add a newline afer the end of the progress bar

        print("waiting for {} threads to return...".format(len(hash_threads)))

        all_threads_done = False
        last_done_count = 0
        while not all_threads_done:
            done_count = 0
            for thread in hash_threads:
                done_count += thread.done()
            all_threads_done = (done_count == len(hash_threads))
            if last_done_count != done_count:
                last_done_count = done_count
                progress2.print_progress_bar(done_count / len(hash_threads), done_count)
            else:
                sleep(min((progress2.get_ETA(done_count / len(hash_threads))/100, 1))) # not sure if this is working at the moment

        print("") # to add a newline after the end of the progress bar

        print("processing hashes...")

        file_hash_groups: list[tuple[str]] = [thread.result() for thread in hash_threads]

    file_hashes: list[str] = list()
    [file_hashes.extend(file_hash_group) for file_hash_group in file_hash_groups]
    progress = progress_bar(100, rate_units="files")
    i = -1

    for filepath in ordered_filepaths:
        i += 1
        first_filepaths = (filepath in filepaths1_set)
        if first_filepaths:
            index = 0
        else:
            index = 1
        file_size = filepath_sizes[filepath]
        file_hash1 = filepath_hash1s[filepath]
        file_hash2 = file_hashes[i]
        if file_hash2 == "":
            continue # happens if there was an error getting the hash
        try:
            filepaths_grouped_by_size_hash2[(file_size, file_hash1, file_hash2)][index].append(filepath)
        except KeyError: # can't append if the list hadn't been created
            if first_filepaths:
                filepaths_grouped_by_size_hash2[(file_size, file_hash1, file_hash2)] = ([filepath], list())
            else:
                filepaths_grouped_by_size_hash2[(file_size, file_hash1, file_hash2)] = (list(), [filepath])
        if i % files_per_group == 0:
            progress.print_progress_bar((i+1) / files_to_process, i+1)
    progress.print_progress_bar(1, i+1)

    print("") # to add a newline after the end of the progress bar

    print("counting hash matches (exact duplicates)...")

    hash2_match_filepathss: tuple[list[str], list[str]] = (list(), list()) # same format as filepathss

    for filehash2 in filepaths_grouped_by_size_hash2.keys():
        if ((not paths_are_identical and (len(filepaths_grouped_by_size_hash2[filehash2][0]) > 0 and len(filepaths_grouped_by_size_hash2[filehash2][1]) > 0)) or
           (paths_are_identical and len(filepaths_grouped_by_size_hash2[filehash2][0]) > 1)):
            # then there are matches
            hash2_match_filepathss[0].extend(filepaths_grouped_by_size_hash2[filehash2][0])
            hash2_match_filepathss[1].extend(filepaths_grouped_by_size_hash2[filehash2][1])
    
    files_to_process = len(hash2_match_filepathss[0]) + len(hash2_match_filepathss[1])

    print("{} duplicate files".format(files_to_process))

    duplicate_file_matches: list[tuple[tuple[str], tuple[str]]] = list()

    for filehash2 in filepaths_grouped_by_size_hash2.keys():
        if ((not paths_are_identical and (len(filepaths_grouped_by_size_hash2[filehash2][0]) > 0 and len(filepaths_grouped_by_size_hash2[filehash2][1]) > 0)) or
           (paths_are_identical and len(filepaths_grouped_by_size_hash2[filehash2][0]) > 1)):
            # then there are matches
            path1_match_files = tuple(filepaths_grouped_by_size_hash2[filehash2][0])
            if paths_are_identical:
                duplicate_file_matches.append((path1_match_files, path1_match_files))
            else:
                path2_match_files = tuple(filepaths_grouped_by_size_hash2[filehash2][1])
                duplicate_file_matches.append((path1_match_files, path2_match_files))

    return tuple(duplicate_file_matches)


def __get_multiple_file_hashes(filepaths: tuple[str], buffer_chunk_size: int = 16777216, only_read_one_chunk: bool = True) -> tuple[str]:
    """
    calls get_hash for each filepath in filepaths, returns the tuple of the results
    """
    file_hashes = list()

    for filepath in filepaths:
        try:
            file_hash = get_hash(filepath, buffer_chunk_size, only_read_one_chunk)
        except: # couldn't get the file hash for some reason
            file_hash = ""
        file_hashes.append(file_hash)

    return tuple(file_hashes)        


def __get_multiple_file_sizes(filepaths: tuple[str]) -> tuple[int]:
    """
    gets the size of each file in filepaths
    """
    ordered_file_sizes = list()

    for filepath in filepaths:
        try:
            file_size = os.stat(filepath).st_size
        except:
            file_size = 0 # couldn't get filesize for some reason
        ordered_file_sizes.append(file_size)

    return tuple(ordered_file_sizes)


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

    grouped_filepaths = [tuple(filepaths[i:i+files_per_group]) if i+files_per_group < len(filepaths) else filepaths[i:] for i in range(0, len(filepaths), files_per_group)]

    threads = list()
    total_size = 0
    with ThreadPoolExecutor() as executor:
        for filepaths in grouped_filepaths:
            thread = executor.submit(get_size_of_files_singlethreaded, filepaths)
            threads.append(thread)
        wait(threads)
        total_size += sum(thread.result() for thread in threads)

    return total_size


def main():
    start_time = time()
    files = get_all_files_in_folder("C:/")
    print("{} files".format(len(files)))
    #files = limit_files_by_size(files, 1024*1024)
    #print("{} files after limiting by size".format(len(files)))
    #size = get_size_of_files(files)
    #print("files are {} bytes total in size".format(size))
    #files2 = get_all_files_in_folder("C:/Users/onebi/Documents")
    #print("{} files".format(len(files2)))
    #files2 = limit_files_by_size(files2, 1024*1024)
    #print("{} files after limiting by size".format(len(files2)))

    #duplicates = get_duplicate_files(files, files2)

    #import csv
    #with open("duplicate_files.csv", "w", newline="") as file:
    #    csv_writer = csv.writer(file)
    #    for duplicate_pair in duplicates:
    #        try:
    #            csv_writer.writerow(duplicate_pair)
    #        except UnicodeEncodeError:
    #            pass
    
    print("{} seconds".format(time() - start_time))

if __name__ == "__main__":
    main()
