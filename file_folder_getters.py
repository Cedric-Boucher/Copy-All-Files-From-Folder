from time import time, sleep
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from progress_bar import progress_bar


def get_immediate_subfolders(path) -> tuple[str]: # TODO move to Filelist
    """
    returns a tuple of the strings of absolute paths of the immediate subfolders of the input path
    """
    assert (os.path.exists(path)), "path does not exist"

    subfolders_and_files = os.listdir(os.path.abspath(path))
    subfolder_paths: list[str] = [os.path.abspath(path+"/"+subfolder_or_file) for subfolder_or_file in subfolders_and_files if os.path.isdir(os.path.abspath(path+"/"+subfolder_or_file))]

    return tuple(subfolder_paths)


def clean_subfolders(folderpath: str, unique_folders: set[str]) ->  None: # FIXME currently non-functional
    """
    recursively deletes any folders in folderpath's subfolders (self-inclusive) that are empty
    and are included in unique_folders
    """
    assert (os.path.exists(folderpath)), "folderpath didn't exist"
    assert (isinstance(unique_folders, set)), "unique_folders was not a set"

    folderpath = os.path.abspath(folderpath)
    subfolders = get_immediate_subfolders(folderpath)
    for subfolder in subfolders: # recursively clean subfolders
        subfolder_in_unique_folders = (len(tuple(unique_folders.intersection(set((subfolder,))))) > 0)
        if subfolder_in_unique_folders:
            clean_subfolders(subfolder, unique_folders)

    folder_in_unique_folders = (len(tuple(unique_folders.intersection(set((folderpath,))))) > 0)
    if is_folder_empty(folderpath) and len(get_immediate_subfolders(folderpath)) == 0 and folder_in_unique_folders:
        # if folder is now empty, we can delete this one
        os.rmdir(folderpath)

    return None


def get_duplicate_files(filepaths1: tuple[str], filepaths2: tuple[str], files_per_group: int = 100) -> tuple[tuple[tuple[str], tuple[str]]]: # TODO move to Filelist
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
                try:
                    sleep(min((progress2.get_ETA(done_count / len(hash_threads))/100, 1)))
                except ZeroDivisionError:
                    sleep(0.01)

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
                try:
                    sleep(min((progress2.get_ETA(done_count / len(hash_threads))/100, 1))) # not sure if this is working at the moment
                except ZeroDivisionError:
                    sleep(0.01)

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
    total_extra_size = 0

    for filehash2 in filepaths_grouped_by_size_hash2.keys():
        if ((not paths_are_identical and (len(filepaths_grouped_by_size_hash2[filehash2][0]) > 0 and len(filepaths_grouped_by_size_hash2[filehash2][1]) > 0)) or
           (paths_are_identical and len(filepaths_grouped_by_size_hash2[filehash2][0]) > 1)):
            # then there are matches
            path1_match_files = tuple(filepaths_grouped_by_size_hash2[filehash2][0])
            if paths_are_identical:
                duplicate_file_matches.append((path1_match_files, path1_match_files))
                total_extra_size += filehash2[0] * (len(duplicate_file_matches[-1][0]) - 1) # size of (total matches minus the one to keep)
            else:
                path2_match_files = tuple(filepaths_grouped_by_size_hash2[filehash2][1])
                duplicate_file_matches.append((path1_match_files, path2_match_files))
                total_extra_size += filehash2[0] * (len(duplicate_file_matches[-1][0]) + len(duplicate_file_matches[-1][1]) - 1) # size of (total matches minus the one to keep)

    print("total size in bytes that can be freed by deleting extra copies of files: {}".format(total_extra_size))

    return tuple(duplicate_file_matches)


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


def main():
    start_time = time()
    
    print("{} seconds".format(time() - start_time))

if __name__ == "__main__":
    main()
