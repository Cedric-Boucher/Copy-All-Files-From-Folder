from time import time
import os
from concurrent.futures import ThreadPoolExecutor
from filecmp import cmp as compare_files
from progress_bar import progress_bar

def get_file_extensions(path) -> tuple[str]:
    """
    returns a tuple of all file extensions is a folder and subfolders
    """
    assert (os.path.exists(path)), "path does not exist"

    file_extensions = list()

    with ThreadPoolExecutor() as executor:
        for _, _, files in os.walk(os.path.abspath(path)):
            new_file_extensions = executor.submit(get_file_extensions_unit_processor, files)
            file_extensions.extend([extension for extension in new_file_extensions.result() if extension not in file_extensions])

    return tuple(file_extensions)


def get_file_extensions_unit_processor(files: list[str]) -> tuple[str]:
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


def get_duplicate_files(path1, path2) -> tuple[tuple[str, str]]:
    """
    returns a tuple of all the files that are duplicated between path1 and path2,
    as a tuple of the full path of the first instance, and the full path of the second instance.

    if path1 and path2 are the same, will ignore case when filenames match, of course.
    """
    assert (os.path.exists(path1)), "path1 does not exist"
    assert (os.path.exists(path2)), "path2 does not exist"

    paths_are_identical = (os.path.abspath(path1) == os.path.abspath(path2))
    duplicate_files: list[tuple[str, str]] = list()
    potential_duplicate_file_paths: dict[int, tuple[list[str]]] = dict()
    # keys are size, values are tuples of size 2, first files from path1 then files from path2,
    # inside that tuple is a list of the full filepaths of any files of this size

    for file_path1, _, files1 in os.walk(os.path.abspath(path1)):
        full_paths = [os.path.abspath(file_path1+"/"+file) for file in files1 if os.path.exists(file_path1+"/"+file)]
        sizes = [os.stat(file).st_size for file in full_paths]
        for i in range(len(full_paths)):
            try:
                potential_duplicate_file_paths[sizes[i]][0].append(full_paths[i])
            except KeyError:
                potential_duplicate_file_paths[sizes[i]] = ([full_paths[i]], list())

    if not paths_are_identical:
        for file_path2, _, files2 in os.walk(os.path.abspath(path2)):
            full_paths = [os.path.abspath(file_path2+"/"+file) for file in files2 if os.path.exists(file_path2+"/"+file)]
            sizes = [os.stat(file).st_size for file in full_paths]
            for i in range(len(full_paths)):
                try:
                    potential_duplicate_file_paths[sizes[i]][1].append(full_paths[i])
                except KeyError:
                    potential_duplicate_file_paths[sizes[i]] = (list(), [full_paths[i]])
    
    progress = progress_bar(100, rate_units="keys")
    total_keys = len(potential_duplicate_file_paths.keys())
    current_key_index = 0

    for key in potential_duplicate_file_paths.keys(): # TODO first check shallow to get rid of extra files and then check deep (with multithreading?)
        if paths_are_identical:
            # then duplicates are only in the first element of the tuple
            potential_duplicates: tuple[list[str], list[str]] = (potential_duplicate_file_paths[key][0], potential_duplicate_file_paths[key][0])
        else:
            potential_duplicates: tuple[list[str], list[str]] = (potential_duplicate_file_paths[key][0], potential_duplicate_file_paths[key][1])
        for file1 in potential_duplicates[0]:
            for file2 in potential_duplicates[1]:
                if file1 != file2: # don't need to compare a file to itself
                    files_are_identical = compare_files(file1, file2, shallow=False)
                    if files_are_identical:
                        duplicate_files.append((file1, file2))

        current_key_index += 1
        progress.print_progress_bar(current_key_index/total_keys, current_key_index)

    print("") # to add a newline after the end of the progress bar

    return tuple(duplicate_files)


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
            new_num_files = executor.submit(get_num_files_in_folder_unit_processor, files, file_extensions, start_with)
            num_files += new_num_files.result()

    return num_files


def get_num_files_in_folder_unit_processor(files: list[str], end_with: tuple[str], start_with: tuple[str]) -> int:
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
            new_folder_size = executor.submit(get_size_of_folder_unit_processor, files, parent_path, start_with, file_extensions)
            total_size += new_folder_size.result()

    return total_size


def get_size_of_folder_unit_processor(files: list[str], parent_path: str, start_with: tuple[str], end_with: tuple[str]) -> int:
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
    duplicates = get_duplicate_files("K:/General", "K:/General")

    import csv
    with open("duplicate_files.csv", "w", newline="") as file:
        csv_writer = csv.writer(file)
        for duplicate_pair in duplicates:
            csv_writer.writerow(duplicate_pair)
    print("{} seconds".format(time() - start_time))
