import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait

class Filelist():
    """
    Stores information about all files in some input path that satisfy input requirements.
    Cannot be modified once created.
    """
    DEFAULT_MAX_FILESIZE = 2**126
    FILES_PER_MULTITHREADED_COMPUTE_GROUP = 10000 # for compute bound groups
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

        self.__limit_filelist_by_file_extensions()
        self.__limit_filelist_by_file_starts()
        self.__limit_files_by_size()

        return None


    def __create_size_list(self) -> None:
        """
        populates self.__filesizes
        """
        if len(self.__filesizes) != 0:
            return None # we have already generated size list

        self.__create_filelist()

        self.__filesizes = tuple([os.stat(filepath).st_size for filepath in self.__filepaths]) # FIXME may fail if a file has disappeared

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


    def __limit_files_by_size_singlethreaded(self, start_index: int, stop_index: int) -> tuple[int]:
        """
        limits files to only keep files between min_size and max_size
        min and maxes are inclusive

        does not modify self. to be used only be the multithreaded self.__limit_files_by_size method
        """
        indices_to_keep: list[int] = [index for index in range(start_index, stop_index) if (self.__filesizes[index] >= self.__min_filesize and self.__filesizes[index] <= self.__max_filesize)]

        return tuple(indices_to_keep)


    def __limit_files_by_size(self) -> None:
        """
        limits files to only keep files between min_size and max_size
        min and maxes are inclusive
        """
        files_per_group = self.FILES_PER_MULTITHREADED_COMPUTE_GROUP
        if self.__min_filesize == 0 and self.__max_filesize == self.DEFAULT_MAX_FILESIZE:
            return None # no need to limit by size

        self.__create_size_list()

        indices_to_keep: list[int] = list()

        start_stop_index_groups: list[tuple[int, int]] = [tuple(i, i+files_per_group) if i+files_per_group < len(self.__filepaths) else self.__filepaths[i:] for i in range(0, len(self.__filepaths), files_per_group)]

        threads = list()

        with ProcessPoolExecutor() as executor:
            for start_index, stop_index in start_stop_index_groups:
                thread = executor.submit(self.__limit_files_by_size_singlethreaded, start_index, stop_index)
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
