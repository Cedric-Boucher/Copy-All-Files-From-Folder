from time import time
import os
from concurrent.futures import ThreadPoolExecutor
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


def get_file_extensions(path) -> tuple[str]:
    """
    returns a tuple of all file extensions is a folder and subfolders
    """
    assert (os.path.exists(path)), "path does not exist"

    file_extensions = list()

    with ThreadPoolExecutor() as executor:
        for _, _, files in os.walk(os.path.abspath(path)):
            new_file_extensions = executor.submit(__get_file_extensions_unit_processor, files)
            file_extensions.extend([extension for extension in new_file_extensions.result() if extension not in file_extensions])

    return tuple(file_extensions)


def __get_file_extensions_unit_processor(files: list[str]) -> tuple[str]:
    """
    unit multithreaded processor for get_file_extenions,
    do not use by itself
    """
    file_extensions = list()

    for file in files:
        filename_parts = file.split(".")
        file_extension = "."+filename_parts[-1]
        if file_extension not in file_extensions:
            file_extensions.append(file_extension)

    return file_extensions


def get_small_or_large_files(path, size_cutoff: int, is_max: bool = True) -> tuple[tuple[str, int]]:
    """
    gets all files from path which:
        are less than or equal to the size cutoff if is_max is True
        are greater than or equal to the size cutoff if is_max is False
    
    returns a tuple of tuples of the full filepaths and their corresponding filesize
    """
    assert (os.path.exists(path)), "path does not exist"
    assert (type(size_cutoff) == int), "size cutoff was not an int"
    assert (size_cutoff >= 0), "size cutoff was negative"
    assert (type(is_max) == bool), "is_max was not a bool"

    files_sizes_pairs: list[tuple[str, int]] = list()

    with ThreadPoolExecutor() as executor:
        for path_to_file, _, files in os.walk(os.path.abspath(path)):
            files = [os.path.abspath(path_to_file+"/"+file) for file in files if os.path.exists(path_to_file+"/"+file)]
            thread_result = executor.submit(__get_small_or_large_files_unit_processor, files, size_cutoff, is_max)
            files_sizes_pairs.extend(thread_result.result())

    return tuple(files_sizes_pairs)


def __get_small_or_large_files_unit_processor(files: list[str], size_cutoff: int, is_max: bool) -> list[tuple[str, int]]:
    """
    unit multithreaded processor for get_small_or_large_files,
    do not use by itself
    """
    files_sizes_pairs: list[tuple[str, int]] = list()
    for file in files:
        file_size = os.stat(file).st_size
        if (is_max and file_size <= size_cutoff) or (not is_max and file_size >= size_cutoff):
            files_sizes_pairs.append((file, file_size))
    
    return files_sizes_pairs


def get_duplicate_files(path1, path2) -> tuple[tuple[str, str]]:
    """
    returns a tuple of all the files that are duplicated between path1 and path2,
    as a tuple of the full path of the first instance, and the full path of the second instance.

    if path1 and path2 are the same, will ignore case when filenames match, of course.

    does not return files that have 0 bytes size.
    """
    assert (os.path.exists(path1)), "path1 does not exist"
    assert (os.path.exists(path2)), "path2 does not exist"

    paths_are_identical = (os.path.abspath(path1) == os.path.abspath(path2))
    duplicate_files: list[tuple[str, str]] = list()
    file_paths_by_size: dict[int, tuple[list[tuple[str, str]]]] = dict()
    # keys are size, values are tuples of size 2, first files from path1 then files from path2,
    # inside that tuple is a list of tuples containing
    # the full filepaths of any files of this size, and their sha256 hashes

    print("counting files in folders...")

    files_in_path1 = get_num_files_in_folder(path1)
    if not paths_are_identical:
        files_in_path2 = get_num_files_in_folder(path2)

    print("getting filesizes and hashes...")

    progress = progress_bar(100, rate_units="files")
    file_counter = 0

    for file_path1, _, files1 in os.walk(os.path.abspath(path1)):
        full_paths = [os.path.abspath(file_path1+"/"+file) for file in files1 if os.path.exists(file_path1+"/"+file)]
        sizes = [os.stat(file).st_size for file in full_paths]
        file_counter += len(full_paths)
        hashes = [get_hash(file, buffer_chunk_size=1048576, only_read_one_chunk=True) for file in full_paths]
        for i in range(len(full_paths)):
            if sizes[i] == 0:
                continue # all files of 0 bytes would match, which is very slow and unnecessary
            try:
                file_paths_by_size[sizes[i]][0].append((full_paths[i], hashes[i]))
            except KeyError: # can't append if the list hadn't been created
                file_paths_by_size[sizes[i]] = ([(full_paths[i], hashes[i])], list())
        progress.print_progress_bar(file_counter / files_in_path1, file_counter)

    print("") # to add a newline after the end of the progress bar

    if not paths_are_identical:
        progress = progress_bar(100, rate_units="files")
        file_counter = 0
        for file_path2, _, files2 in os.walk(os.path.abspath(path2)):
            full_paths = [os.path.abspath(file_path2+"/"+file) for file in files2 if os.path.exists(file_path2+"/"+file)]
            sizes = [os.stat(file).st_size for file in full_paths]
            file_counter += len(full_paths)
            hashes = [get_hash(file, buffer_chunk_size=1048576, only_read_one_chunk=True) for file in full_paths]
            for i in range(len(full_paths)):
                if sizes[i] == 0:
                    continue # all files of 0 bytes would match, which is very slow and unnecessary
                try:
                    file_paths_by_size[sizes[i]][1].append((full_paths[i], hashes[i]))
                except KeyError: # can't append if the list hadn't been created
                    file_paths_by_size[sizes[i]] = (list(), [(full_paths[i], hashes[i])])
            progress.print_progress_bar(file_counter / files_in_path2, file_counter)

    print("") # to add a newline after the end of the progress bar
    print("finding duplicates...")

    progress = progress_bar(100, rate_units="keys")
    total_keys = len(file_paths_by_size.keys())
    current_key_index = 0

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
            # only compare files byte-by-byte if they have the same filetype, aren't the same path, and have the same hash
            for file2 in files_to_compare:
                try:
                    files_are_identical = compare_files(file1, file2, shallow=False)
                except:
                    files_are_identical = False
                # verify that files are actually identical all the way through, byte for byte, since
                # the hash only read the first little bit of each file
                if files_are_identical:
                    duplicate_files.append((file1, file2))

        current_key_index += 1
        progress.print_progress_bar(current_key_index/total_keys, current_key_index)

    print("") # to add a newline after the end of the progress bar

    return tuple(duplicate_files)


def get_hash(file, buffer_chunk_size: int = 16777216, only_read_one_chunk: bool = False) -> str:
    """
    gets the hash (sha256) of a file
    default buffer size of 16MiB
    """
    assert (os.path.exists(file)), "file doesn't exist"
    assert (type(buffer_chunk_size) == int), "buffer chunk size needs to be an int"
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


def get_num_files_in_folder(path, file_extensions: tuple[str] = (), start_with: tuple[str] = ()) -> int:
    """
    Counts the number of files in a directory and subdirectories using os.walk
    set print_stats_every_x_seconds to -1 to never print
    if file_extensions is an empty tuple, will not check file extensions,
    if start_with is an empty tuple, will not check what a filename starts with.
    if they are set, only files that match all of those conditions will be counted
    file_extensions is just an endswith check, so I reccomend including the period

    original singlethreaded version (faster in my testing)
    """
    assert (os.path.exists(path)), "path does not exist"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""

    num_files = 0
    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.endswith(file_extensions) and file.startswith(start_with):
                num_files += 1

    return num_files


def get_num_files_in_folder_multithreaded(path, file_extensions: tuple[str] = (), start_with: tuple[str] = ()) -> int:
    """
    Counts the number of files in a directory and subdirectories using os.walk
    if file_extensions is an empty tuple, will not check file extensions,
    if start_with is an empty tuple, will not check what a filename starts with.
    if they are set, only files that match all of those conditions will be counted
    file_extensions is just an endswith check, so I reccomend including the period

    in my testing this has actually been significantly slower than the singlethreaded version
    """
    assert (os.path.exists(path)), "path does not exist"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""

    num_files = 0
    with ThreadPoolExecutor() as executor:
        for _, _, files in os.walk(os.path.abspath(path)):
            new_num_files = executor.submit(__get_num_files_in_folder_unit_processor, files, file_extensions, start_with)
            num_files += new_num_files.result()

    return num_files


def __get_num_files_in_folder_unit_processor(files: list[str], end_with: tuple[str], start_with: tuple[str]) -> int:
    """
    returns number of files with matching begin and end string
    do not use on its own
    """
    num_files = 0

    for file in files:
        if file.endswith(end_with) and file.startswith(start_with):
            num_files += 1

    return num_files


def get_size_of_folder(path, file_extensions: tuple[str] = (), start_with: tuple[str] = ()) -> int:
    """
    gets the sum of all file sizes in path and all subfolders, that match file_extensions and start_with
    set print_stats_every_x_seconds to -1 to never print
    if file_extensions is an empty tuple, will not check file extensions,
    if start_with is an empty tuple, will not check what a filename starts with.
    if they are set, only files that match all of those conditions will be counted
    file_extensions is just an endswith check, so I reccomend including the period

    original singlethreaded version (faster in my testing)
    """
    assert (os.path.exists(path)), "path does not exist"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""

    total_size = 0
    for parent_path, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.endswith(file_extensions) and file.startswith(start_with):
                try:
                    total_size += os.stat(os.path.abspath(parent_path+"/"+file))[6] # bytes filesize
                except OSError:
                    pass

    return total_size


def get_size_of_folder_multithreaded(path, file_extensions: tuple[str] = (), start_with: tuple[str] = ()) -> int:
    """
    gets the sum of all file sizes in path and all subfolders, that match file_extensions and start_with
    set print_stats_every_x_seconds to -1 to never print
    if file_extensions is an empty tuple, will not check file extensions,
    if start_with is an empty tuple, will not check what a filename starts with.
    if they are set, only files that match all of those conditions will be counted
    file_extensions is just an endswith check, so I reccomend including the period

    in my testing this has actually been significantly slower than the singlethreaded version
    """
    assert (os.path.exists(path)), "path does not exist"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""

    total_size = 0
    with ThreadPoolExecutor() as executor:
        for parent_path, _, files in os.walk(os.path.abspath(path)):
            new_folder_size = executor.submit(__get_size_of_folder_unit_processor, files, parent_path, start_with, file_extensions)
            total_size += new_folder_size.result()

    return total_size


def __get_size_of_folder_unit_processor(files: list[str], parent_path: str, start_with: tuple[str], end_with: tuple[str]) -> int:
    """
    returns the total size of the given files
    do not use on its own
    """
    total_size = 0

    for file in files:
        if file.endswith(end_with) and file.startswith(start_with):
            try:
                total_size += os.stat(os.path.abspath(parent_path+"/"+file))[6] # bytes filesize
            except OSError: # tried to access a restricted file
                pass

    return total_size


if __name__ == "__main__":
    start_time = time()
    duplicates = get_duplicate_files("C:/", "C:/")

    import csv
    with open("duplicate_files.csv", "w", newline="") as file:
        csv_writer = csv.writer(file)
        for duplicate_pair in duplicates:
            try:
                csv_writer.writerow(duplicate_pair)
            except UnicodeEncodeError:
                pass
    print("{} seconds".format(time() - start_time))
