from time import time
import os
from concurrent.futures import ThreadPoolExecutor

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


def get_file_extensions_unit_processor(files: list) -> tuple[str]:
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


def get_num_files_in_folder_unit_processor(files: list, end_with: tuple[str], start_with: tuple[str]) -> int:
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
                total_size += os.stat(os.path.abspath(parent_path+"/"+file))[6] # bytes filesize

    return total_size


if __name__ == "__main__":
    start_time = time()
    print(get_num_files_in_folder("C:/"))
    print("{} seconds".format(time() - start_time))
