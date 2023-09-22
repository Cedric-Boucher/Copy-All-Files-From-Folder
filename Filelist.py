import os

class Filelist():
    """
    Stores information about all files in some input path that satisfy input requirements
    """

    def __init__(self, input_folder, file_extensions: tuple[str] = (), start_with: tuple[str] = (), min_filesize: int = 0, max_filesize: int = 2**64) -> None:
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
        self.__create_filelist()
        self.__filesizes: tuple[int] = tuple() # number of bytes, maps 1:1 with filepaths

        return None


    def __create_filelist(self) -> None:
        """
        populates self.__filepaths
        """
        files = list()

        for path_to_file, _, sub_files in os.walk(self.__input_folder):
            files.extend([os.path.abspath(path_to_file+"/"+sub_file) for sub_file in sub_files])

        self.__filepaths = tuple(files)

        self.__limit_filelist_by_file_extensions()
        self.__limit_filelist_by_file_starts()

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


    def get_filepaths(self) -> tuple[str]:
        """
        returns the list (well, a tuple) of filepaths
        """

        return self.__filepaths
