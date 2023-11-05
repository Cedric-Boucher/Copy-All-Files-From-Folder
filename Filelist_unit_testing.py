import unittest
from time import time
from copy import deepcopy
from Filelist import Filelist
import os
from pprint import pprint


TEST_FOLDER_RELATIVE_PATH = "FILELIST_TESTING"
TEST_FILE_SIZE = 1024**2 # in bytes
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
from Filelist_expected_test_results import EXPECTED_TEST_RESULTS


def create_test_setup():
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

    test_files = tuple([os.path.join(TEST_FOLDER_RELATIVE_PATH, file) for file in TEST_FILES])

    for file_path in test_files:
        file_size = TEST_FILE_SIZE
        bytes_to_write = bytes([0 for _ in range(file_size)])
        with open(file_path, "wb") as file_handle:
            file_handle.write(bytes_to_write)

    return



class test_Filelist():
    def setUp(self) -> None:
        """self.expected_results and self.test_filelist should be set by subclass"""
        self.test_filelist = None
        self.expected_results = None
        self.maxDiff = None

    def tearDown(self) -> None:
        del self.test_filelist
        del self.expected_results

    def test_obtaining_filepaths(self) -> None:
        result = self.test_filelist.get_filepaths()
        self.assertEqual(result, self.expected_results[0])

    def test_obtaining_filepaths_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.get_filepaths()
        self.assertEqual(result, self.expected_results[1])

    def test_obtaining_file_extensions(self) -> None:
        result = self.test_filelist.get_file_extensions()
        self.assertEqual(result, self.expected_results[2])

    def test_obtaining_file_extensions_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.get_file_extensions()
        self.assertEqual(result, self.expected_results[3])

    def test_obtaining_file_extensions_after_obtaining_file_extensions(self) -> None:
        self.test_filelist.get_file_extensions()
        result = self.test_filelist.get_file_extensions()
        self.assertEqual(result, self.expected_results[4])

    def test_obtaining_file_sizes(self) -> None:
        result = self.test_filelist.get_filesizes()
        self.assertEqual(result, self.expected_results[5])

    def test_obtaining_file_sizes_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.get_filesizes()
        self.assertEqual(result, self.expected_results[6])

    def test_obtaining_file_sizes_after_obtaining_file_sizes(self) -> None:
        self.test_filelist.get_filesizes()
        result = self.test_filelist.get_filesizes()
        self.assertEqual(result, self.expected_results[7])

    def test_obtaining_has_files(self) -> None:
        result = self.test_filelist.does_folder_have_files()
        self.assertEqual(result, self.expected_results[8])

    def test_obtaining_has_files_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.does_folder_have_files()
        self.assertEqual(result, self.expected_results[9])

    def test_obtaining_has_files_after_obtaining_has_files(self) -> None:
        self.test_filelist.does_folder_have_files()
        result = self.test_filelist.does_folder_have_files()
        self.assertEqual(result, self.expected_results[10])

    def test_obtaining_file_extensions_singlethreaded(self) -> None:
        result = self.test_filelist.get_file_extensions_singlethreaded()
        self.assertEqual(result, self.expected_results[11])

    def test_obtaining_file_extensions_singlethreaded_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.get_file_extensions_singlethreaded()
        self.assertEqual(result, self.expected_results[12])

    def test_obtaining_file_extensions_singlethreaded_after_obtaining_file_extensions(self) -> None:
        self.test_filelist.get_file_extensions_singlethreaded()
        result = self.test_filelist.get_file_extensions_singlethreaded()
        self.assertEqual(result, self.expected_results[13])

    def test_obtaining_file_hashes(self) -> None:
        result = self.test_filelist.get_filehashes()
        self.assertEqual(result, self.expected_results[14])

    def test_obtaining_file_hashes_after_obtaining_filepaths(self) -> None:
        self.test_filelist.get_filepaths()
        result = self.test_filelist.get_filehashes()
        self.assertEqual(result, self.expected_results[15])

    def test_obtaining_file_hashes_after_obtaining_file_hashes(self) -> None:
        self.test_filelist.get_filehashes()
        result = self.test_filelist.get_filehashes()
        self.assertEqual(result, self.expected_results[16])



class test_Filelist_no_arguments(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_file_extensions(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, file_extensions=(".png", ".jpg"))
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_file_starts(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, start_with=("file1",))
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_file_extensions_and_starts(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, file_extensions=(".png", ".jpg"), start_with=("file1",))
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_min_filesize_greater(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, min_filesize=TEST_FILE_SIZE+1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_min_filesize_lesser(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, min_filesize=TEST_FILE_SIZE-1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_max_filesize_greater(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, max_filesize=TEST_FILE_SIZE+1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_max_filesize_lesser(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, max_filesize=TEST_FILE_SIZE-1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_min_and_max_filesize_bounding(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, min_filesize=TEST_FILE_SIZE-1, max_filesize=TEST_FILE_SIZE+1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_min_and_max_filesize_invalid(test_Filelist, unittest.TestCase): # this will throw exceptions
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, min_filesize=TEST_FILE_SIZE+1, max_filesize=TEST_FILE_SIZE-1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_everything_1(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, file_extensions=(".png", ".jpg"), start_with=("file1",), min_filesize=TEST_FILE_SIZE-1, max_filesize=TEST_FILE_SIZE+1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_everything_2(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, file_extensions=(".png", ".jpg"), start_with=("file1",), min_filesize=TEST_FILE_SIZE+1, max_filesize=TEST_FILE_SIZE+1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]


class test_Filelist_everything_3(test_Filelist, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_filelist = Filelist(TEST_FOLDER_RELATIVE_PATH, file_extensions=(".png", ".jpg"), start_with=("file1",), min_filesize=TEST_FILE_SIZE-1, max_filesize=TEST_FILE_SIZE-1)
        self.expected_results = EXPECTED_TEST_RESULTS[0]



if __name__ == "__main__":
    create_test_setup()
    unittest.main()
