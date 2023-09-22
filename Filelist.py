import os

class Filelist():
    """
    Stores information about all files in some input path that satisfy input requirements
    """

    def __init__(self, input_folder, file_extensions: tuple[str] = (), start_with: tuple[str] = (), min_filesize: int = 0, max_filesize: int = 2**64) -> None:
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

        self.__filepaths: tuple[str] = tuple()
        self.__get_files()

    def __get_files(self) -> None:
        """
        populates self.__filepaths
        """
        files = list()

        for path_to_file, _, sub_files in os.walk(self.__input_folder):
            files.extend([os.path.abspath(path_to_file+"/"+sub_file) for sub_file in sub_files])

        self.__filepaths = tuple(files)

