from time import time
from copy import deepcopy
from Filelist import Filelist
import os
import random


TEST_FOLDER_RELATIVE_PATH = "FILELIST_TESTING"
TEST_FILE_SIZE_BOUNDS = (1024, 1024**2) # in bytes
TEST_SUBFOLDERS = ("test1", "test2", "test3", "test1/test11")
TEST_FILES = (
    "test1/file1.png",
    "test1/file2.jpg",
    "test1/file3.bmp",
    "test2/file1.png",
    "test2/file2.bmp",
    "test2/file3.jpg",
    "test1/test11/file11.qoi"
    )


def create_test_setup() -> dict[str, int]:
    """
    creates a folder, subfolders, and some files specifically for the purposes of unit testing

    returns a dictionary of the test file paths and their corresponding file size in bytes
    """
    if not os.path.exists(TEST_FOLDER_RELATIVE_PATH):
        os.makedirs(TEST_FOLDER_RELATIVE_PATH)

    subfolders = tuple([os.path.join(TEST_FOLDER_RELATIVE_PATH, subfolder) for subfolder in TEST_SUBFOLDERS])

    for subfolder in subfolders:
        if not os.path.exists(subfolder):
            os.makedirs(subfolder)

    test_file_path_sizes: dict[str, int] = dict()

    test_files = tuple([os.path.join(TEST_FOLDER_RELATIVE_PATH, file) for file in TEST_FILES])

    for file_path in test_files:
        file_size = random.randint(TEST_FILE_SIZE_BOUNDS[0], TEST_FILE_SIZE_BOUNDS[1])
        test_file_path_sizes[file_path] = file_size
        bytes_to_write = bytes([0 for _ in range(file_size)])
        with open(file_path, "wb") as file_handle:
            file_handle.write(bytes_to_write)

    return test_file_path_sizes


def test_Filelist():
    """
    runs all tests on Filelist to ensure things are working as expected
    """

    """
    functionality to test:
    - [x] creating Filelist object and using them for each of the items below with various input parameter settings:
        - [x] obtaining filepaths manually
        - [x] obtaining filepaths manually after obtaining filepaths manually
        - [x] obtaining file extensions manually without obtaining filepaths first
        - [x] obtaining file extensions manually after obtaining filepaths manually
        - [x] obtaining file extensions manually after obtaining file extensions manually
        - [x] obtaining file sizes manually without obtaining filepaths first
        - [x] obtaining file sizes manually after obtaining filepaths manually
        - [x] obtaining file sizes manually after obtaining file sizes manually
        - [x] obtaining whether folder has files manually without obtaining filepaths first
        - [x] obtaining whether folder has files manually after obtaining filepaths manually
        - [x] obtaining whether folder has files manually after obtaining whether folder has files
        - [x] obtaining file extensions manually without obtaining filepaths first (singlethreaded)
        - [x] obtaining file extensions manually after obtaining filepaths manually (singlethreaded)
        - [x] obtaining file extensions manually after obtaining file extensions manually (singlethreaded)
        - [x] obtaining file hashes manually without obtaining filepaths first
        - [x] obtaining file hashes manually after obtaining filepaths manually
        - [x] obtaining file hashes manually after obtaining file hashes manually
    """

    test_folder = TEST_FOLDER_RELATIVE_PATH
    filelists = (
        Filelist(test_folder),
        Filelist(test_folder, file_extensions=(".png", ".jpg")),
        Filelist(test_folder, start_with=("file1",)),
        Filelist(test_folder, file_extensions=(".png", ".jpg"), start_with=("file1",)),
        Filelist(test_folder, min_filesize=TEST_FILE_SIZE_BOUNDS[0]*8),
        Filelist(test_folder, max_filesize=int(TEST_FILE_SIZE_BOUNDS[1]/8)),
        Filelist(test_folder, min_filesize=TEST_FILE_SIZE_BOUNDS[0]*8, max_filesize=int(TEST_FILE_SIZE_BOUNDS[1]/8)),
        Filelist(test_folder, file_extensions=(".png", ".jpg"), start_with=("file1",), min_filesize=TEST_FILE_SIZE_BOUNDS[0]*8, max_filesize=int(TEST_FILE_SIZE_BOUNDS[1]/8))
        )

    current_test = [-1, -1]
    test_objects: list[list[Filelist]] = list()
    test_times: list[list[float]] = list()

    for filelist in filelists:
        current_test[0] += 1
        current_test[1] = -1
        test_objects.append(list())
        test_times.append(list())

        # obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filepaths()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining filepaths manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filepaths()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining file extensions manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][2])) # skip getting file extensions again by using test 2 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually after obtaining file sizes manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][5])) # skip getting file sizes again by using test 5 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually after obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually after obtaining whether folder has files manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][8])) # skip checking for files again by using test 8 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually without obtaining filepaths first (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining filepaths manually (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining file extensions manually (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][2])) # skip getting file extensions again by using test 2 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually after obtaining file hashes manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][14])) # skip getting file extensions again by using test 14 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)



if __name__ == "__main__":
    create_test_setup()
    test_Filelist()
