import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from copy import deepcopy

class Filelist():
    """
    Stores information about all files in some input path that satisfy input requirements.
    Cannot be modified once created.
    Cannot modify filesystems in any way, only reads from filesystems through OS.

    Filelist only obtains information from the filesystem (I/O bottlenecked operations) when it is requested.
    However, once information has been obtained, it is saved so that if it is requested again it can be returned instantly.
    Therefore, creating a filelist object is extremely fast, but obtaining the list of filepaths for the first time is I/O bottlenecked.
    """
    DEFAULT_MAX_FILESIZE = 2**126
    FILES_PER_MULTITHREADED_COMPUTE_GROUP = 100000 # for compute bound groups
    FILES_PER_MULTITHREADED_IO_GROUP = 100 # for I/O bound groups

    def __init__(self, input_folder, file_extensions: tuple[str] = (), start_with: tuple[str] = (), min_filesize: int = 0, max_filesize: int = DEFAULT_MAX_FILESIZE) -> None:
        """
        Filelist will initialize by creating the internal data structure with the given inputs here. Once this structure is created, it cannot be edited.
        """
        assert (os.path.exists(input_folder)), "input_folder does not exist"
        assert (isinstance(file_extensions, tuple)), "file_extensions was not a tuple"
        assert (isinstance(start_with, tuple)), "start_with was not a tuple"
        assert (all(isinstance(i, str) for i in file_extensions)), "not all file extensions were strings"
        assert (all(isinstance(i, str) for i in start_with)), "not all file starts were strings"
        assert (isinstance(min_filesize, int)), "min_filesize was not an integer"
        assert (isinstance(max_filesize, int)), "max_filesize was not an integer"
        assert (max_filesize >= min_filesize), "max_filesize was not greater than or equal to min_filesize"

        self.__input_folder = os.path.abspath(input_folder)
        self.__file_extensions: tuple[str] = file_extensions
        self.__start_with: tuple[str] = start_with
        self.__min_filesize: int = min_filesize
        self.__max_filesize: int = max_filesize

        self.__filepaths: tuple[str] = tuple() # full (absolute) filepath strings
        self.__filesizes: tuple[int] = tuple() # number of bytes, maps 1:1 with filepaths
        self.__file_extensions_found: tuple[str] = tuple() # all the unique file extensions found in filepaths
        self.__folder_has_files: bool = None # None until known

        return None


    def __create_filelist(self) -> None:
        """
        populates self.__filepaths
        """
        if len(self.__filepaths) != 0:
            return None # we have already generated filelist

        files: list[str] = list()

        for path_to_file, _, sub_files in os.walk(self.__input_folder):
            files.extend([os.path.abspath(path_to_file+"/"+sub_file) for sub_file in sub_files])

        self.__filepaths = tuple(files)

        # perform limits from least to most expensive in time
        self.__limit_filelist_by_file_extensions()
        self.__limit_filelist_by_file_starts()
        self.__limit_files_by_size()

        return None


    def __create_size_list(self) -> None:
        """
        populates self.__filesizes

        if any files no longer exist (fail to obtain size), they will be removed from filelist
        """
        if len(self.__filesizes) != 0:
            return None # we have already generated size list

        self.__create_filelist()

        filesizes: list[int] = list()
        indices_to_remove: list[int] = list()

        for index in range(len(self.__filepaths)):
            filepath = self.__filepaths[index]
            try:
                filesizes.append(os.stat(filepath).st_size)
            except:
                # os.stat failed, file is no longer accessible
                indices_to_remove.append(index)

        self.__filepaths = tuple([self.__filepaths[index] for index in range(len(self.__filepaths)) if index not in indices_to_remove])
        self.__filesizes = tuple(filesizes)
        # these two are now mapped to each other and of the same length

        return None


    def __limit_filelist_by_file_extensions(self) -> None:
        """
        removes any files in self.__filepaths that do not have one of the file extensions
        """
        if len(self.__file_extensions) == 0:
            return None # no need to limit in this case

        new_filepaths: tuple[str] = tuple([filepath for filepath in self.__filepaths if filepath.endswith(self.__file_extensions)])

        self.__filepaths = new_filepaths

        return None
    

    def __limit_filelist_by_file_starts(self) -> None:
        """
        removes any files in self.__filepaths that do not start with one of the file starts
        """
        if len(self.__start_with) == 0:
            return None # no need to limit in this case

        new_filepaths: list[str] = list()

        for filepath in self.__filepaths:
            filename = os.path.basename(filepath)
            if filename.startswith(self.__start_with):
                new_filepaths.append(filename)

        self.__filepaths = tuple(new_filepaths)

        return None


    def __limit_files_by_size(self) -> None: # TODO create single-threaded version for time comparison
        """
        limits files to only keep files between min_size and max_size
        min and maxes are inclusive
        """
        if self.__min_filesize == 0 and self.__max_filesize == self.DEFAULT_MAX_FILESIZE:
            return None # no need to limit by size

        files_per_group = self.FILES_PER_MULTITHREADED_COMPUTE_GROUP

        self.__create_size_list()

        indices_to_keep: list[int] = list()

        start_stop_index_groups: list[tuple[int, int]] = [(i, i+files_per_group) if i+files_per_group < len(self.__filepaths) else (i, len(self.__filepaths)) for i in range(0, len(self.__filepaths), files_per_group)]

        threads = list()

        with ProcessPoolExecutor() as executor:
            for start_index, stop_index in start_stop_index_groups:
                thread = executor.submit(limit_files_by_size_singlethreaded, self, start_index, stop_index, self.__min_filesize, self.__max_filesize)
                threads.append(thread)
            wait(threads)
            [indices_to_keep.extend(thread.result()) for thread in threads]

        self.__filepaths = tuple([self.__filepaths[index] for index in range(len(self.__filepaths)) if index in indices_to_keep])
        self.__filesizes = tuple([self.__filesizes[index] for index in range(len(self.__filepaths)) if index in indices_to_keep])

        return None


    def get_filepaths(self) -> tuple[str]:
        """
        returns the list (well, a tuple) of filepaths
        """
        self.__create_filelist()

        return self.__filepaths


    def get_filesizes(self) -> tuple[str]:
        """
        returns the list (well, a tuple) of file sizes.
        this is mapped to the tuple of filepaths from get_filepaths()
        """
        self.__create_size_list()

        return self.__filesizes


    def get_file_extensions_singlethreaded(self) -> tuple[str]:
        """
        returns a tuple of all unique file extensions
        faster than the multithreaded version in some (or all) cases
        """
        if len(self.__file_extensions_found) != 0:
            return self.__file_extensions_found # we have already found file extensions

        self.__create_filelist()

        file_extensions = set()

        for index in range(len(self.__filepaths)):
            filename = os.path.basename(self.__filepaths[index]) # get only the filename
            file_extension = "."+filename.split(".")[-1]
            if (not filename.startswith(".")) and (not file_extension.count(" ")): # makes sure files don't start with "." or contain a space in the extension
                file_extensions.add(file_extension)


        self.__file_extensions_found = tuple(file_extensions)

        return self.__file_extensions_found



    def get_file_extensions(self) -> tuple[str]:
        """
        returns a tuple of all unique file extensions
        multithreaded to speed up having to go through potentially millions of files
        """
        if len(self.__file_extensions_found) != 0:
            return self.__file_extensions_found # we have already found file extensions

        files_per_group = self.FILES_PER_MULTITHREADED_COMPUTE_GROUP

        self.__create_filelist()

        file_extensions = set()

        start_stop_index_groups: list[tuple[int, int]] = [(i, i+files_per_group) if i+files_per_group < len(self.__filepaths) else (i, len(self.__filepaths)) for i in range(0, len(self.__filepaths), files_per_group)]

        threads = list()

        with ProcessPoolExecutor() as executor:
            for start_index, stop_index in start_stop_index_groups:
                thread = executor.submit(get_file_extensions_singlethreaded, self, start_index, stop_index)
                threads.append(thread)
            wait(threads)
            [file_extensions.update(thread.result()) for thread in threads]

        self.__file_extensions_found = tuple(file_extensions)

        return self.__file_extensions_found


    def does_folder_have_files(self) -> bool:
        """
        returns True if the folder contains any files,
        False if it has no files (it can contain subfolders)

        ignores any filters. if you want to use filters, check the length of filepaths
        """
        if self.__folder_has_files is not None:
            return self.__folder_has_files

        if len(self.__filepaths) > 0:
            self.__folder_has_files = True
            return True

        # otherwise we aren't sure, so we check

        for _, _, sub_files in os.walk(self.__input_folder):
            if len(sub_files) > 0:
                self.__folder_has_files = True
                return True

        self.__folder_has_files = False

        return False # didn't find any files



def get_file_extensions_singlethreaded(filelist: Filelist, start_index: int, stop_index: int) -> set[str]:
    """
    returns a set of all unique file extensions in the filepaths given

    to be used only be the multithreaded Filelist.get_file_extensions method
    """
    file_extensions = set()

    filepaths = filelist.get_filepaths() # extremely fast since the object already contains the filepaths list

    for index in range(start_index, stop_index):
        filename = os.path.basename(filepaths[index]) # get only the filename
        file_extension = "."+filename.split(".")[-1]
        if (not filename.startswith(".")) and (not file_extension.count(" ")): # makes sure files don't start with "." or contain a space in the extension
            file_extensions.add(file_extension)

    return file_extensions


def limit_files_by_size_singlethreaded(filelist: Filelist, start_index: int, stop_index: int, min_filesize: int, max_filesize: int) -> tuple[int]:
    """
    limits files to only keep files between min_size and max_size
    min and maxes are inclusive

    returns the indices to keep from the filepaths tuple in the filelist object

    to be used only be the multithreaded Filelist.__limit_files_by_size method
    """
    filesizes = filelist.get_filesizes()

    indices_to_keep: list[int] = [index for index in range(start_index, stop_index) if (filesizes[index] >= min_filesize and filesizes[index] <= max_filesize)]

    return tuple(indices_to_keep)



def main():
    from time import time

    t = time()
    filelist = Filelist("/home/d3zyre")
    print("time to create Filelist object: {:.1e} seconds".format(time() - t))
    t = time()

    print("number of files: {}".format(len(filelist.get_filepaths())))
    print("time to get filepaths: {:.1e} seconds".format(time() - t))
    t = time()

    print("number of file extensions: {}".format(len(filelist.get_file_extensions())))
    print("time to get file extensions multithreaded: {:.1e} seconds".format(time() - t))
    t = time()

    filelist.get_file_extensions()
    print("time to get file extensions multithreaded again: {:.1e} seconds".format(time() - t))

    filelist = Filelist("/home/d3zyre") # clear saved file list and file extensions
    filelist.get_filepaths()
    t = time()

    filelist.get_file_extensions_singlethreaded()
    print("time to get file extensions singlethreaded: {:.1e} seconds".format(time() - t))
    t = time()

    filelist.get_file_extensions_singlethreaded()
    print("time to get file extensions singlethreaded again: {:.1e} seconds".format(time() - t))
    t = time()


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
    """
    from time import time

    test_folder = "/home/d3zyre"
    filelists = (
        Filelist(test_folder),
        Filelist(test_folder, file_extensions=(".png", ".jpeg", ".py", ".txt"), start_with=("File", "test")),
        Filelist(test_folder, min_filesize=4096),
        Filelist(test_folder, max_filesize=1024**2),
        Filelist(test_folder, min_filesize=4096, max_filesize=1024**2),
        Filelist(test_folder, file_extensions=(".png", ".jpeg", ".py", ".txt"), start_with=("File", "test"), min_filesize=4096, max_filesize=1024**2)
        )

    current_test = [0, 0]

    for filelist in filelists:
        current_test[0] += 1
        current_test[1] = 0

        # obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test1 = deepcopy(filelist)
        t = time()
        test1.get_filepaths()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining filepaths manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test2 = deepcopy(filelist)
        test2.get_filepaths()
        t = time()
        test2.get_filepaths()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file extensions manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test3 = deepcopy(filelist)
        t = time()
        test3.get_file_extensions()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file extensions manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test4 = deepcopy(filelist)
        test4.get_filepaths()
        t = time()
        test4.get_file_extensions()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file extensions manually after obtaining file extensions manually
        current_test[1] += 1
        print(current_test)
        test5 = deepcopy(filelist)
        test5.get_file_extensions()
        t = time()
        test5.get_file_extensions()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file sizes manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test6 = deepcopy(filelist)
        t = time()
        test6.get_filesizes()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file sizes manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test7 = deepcopy(filelist)
        test7.get_filepaths()
        t = time()
        test7.get_filesizes()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining file sizes manually after obtaining file sizes manually
        current_test[1] += 1
        print(current_test)
        test8 = deepcopy(filelist)
        test8.get_filesizes()
        t = time()
        test8.get_filesizes()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining whether folder has files manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test9 = deepcopy(filelist)
        t = time()
        test9.does_folder_have_files()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining whether folder has files manually after obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test10 = deepcopy(filelist)
        test10.get_filepaths()
        t = time()
        test10.does_folder_have_files()
        print("{:.1e} seconds\n".format(time() - t))

        # obtaining whether folder has files manually after obtaining whether folder has files manually
        current_test[1] += 1
        print(current_test)
        test11 = deepcopy(filelist)
        test11.does_folder_have_files()
        t = time()
        test11.does_folder_have_files()
        print("{:.1e} seconds\n".format(time() - t))


if __name__ == "__main__":
    #main()
    test_Filelist()
