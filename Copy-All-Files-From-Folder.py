"""
compatible with config files versioned V0.3
"""
from shutil import copy2, move, Error
from send2trash import send2trash
import os
from time import time


def get_file_extensions(path) -> tuple[str]:
    """
    returns a tuple of all file extensions is a folder and subfolders
    """
    assert (os.path.exists(path)), "path does not exist"

    file_extensions = list()

    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            filename_parts = file.split(".")
            file_extension = "."+filename_parts[-1]
            if file_extension not in file_extensions:
                file_extensions.append(file_extension)

    return tuple(file_extensions)


def get_num_files_in_folder(path, file_extensions: tuple[str] = (), start_with: tuple[str] = (), print_stats_every_x_seconds = -1) -> int:
    """
    Counts the number of files in a directory and subdirectories using os.walk
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

    num_files = 0
    t = time()
    if print_stats_every_x_seconds != -1:
        print("\nChecking number of files for path "+str(path)+"...\n")
    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.endswith(file_extensions) and file.startswith(start_with):
                num_files += 1
        if time() - t >= print_stats_every_x_seconds and print_stats_every_x_seconds != -1:
            print("\r{} files...".format(num_files), end="")
            t = time()

    return num_files


def progress_bar(progress: float, length: int, start_string = "~<{", end_string = "}>~", fill_char = "/", empty_char = "-", overwrite_previous_bar = True, with_percentage = True) -> None:
    """
    progress is float [0, 1]
    length is character count length of progress bar (not including start and end strings)
    no newline is printed after the progress bar
    """
    assert (type(progress) == float), "type of progress was not float"
    assert (type(length) == int), "type of length was not int"
    assert (progress <= 1), "progress was greater than 1"
    assert (length > 0), "length was negative"
    assert (type(start_string) == str), "start_string was not string"
    assert (type(end_string) == str), "end_string was not string"
    assert (type(fill_char) == str), "fill_char was not string"
    assert (len(fill_char) == 1), "len(fill_char) was not 1"
    assert (type(empty_char) == str), "empty_char was not string"
    assert (len(empty_char) == 1), "len(empty_char) was not 1"

    output_string = start_string
    progress_character_count = int(progress*length)
    output_string += progress_character_count * fill_char
    output_string += (length - progress_character_count) * empty_char
    output_string += end_string
    if overwrite_previous_bar:
        output_string = "\r" + output_string
        print(" " * (length + len(start_string) + len(end_string)), end="")
    print(output_string, end="")
    if with_percentage:
        print(" {:6.2f}%".format(progress*100), end="")

    return None


def move_files(input_folder, output_folder = None, file_extensions: tuple[str] = (), start_with: tuple[str] = (), move_mode: str = "C") -> int:
    """
    move_mode can be either "M" for move, "C" for copy, "T" for trash, "D" for permanently delete

    output_folder should be defined for move_mode C or M, but is unused for T or D

    if file_extensions/start_with is empty tuple then all file extensions will be copied/moved
    
    returns number of errors, prints progress
    """
    assert (move_mode in ["C", "M", "T", "D"]), "move_mode was not one of the options"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"
    assert (os.path.exists(input_folder)), "input_folder does not exist"

    if move_mode in ["C", "M"]:
        if not os.path.exists(output_folder):
            try:
                os.mkdir(output_folder)
            except:
                assert (False), "destination folder didn't exist and couldn't be created"

    number_of_files_total = get_num_files_in_folder(os.path.abspath(input_folder), file_extensions=file_extensions, start_with=start_with)
    number_of_files_processed = 0
    error_count = 0

    if move_mode == "C":
        print("Copying Files from \"{}\" to \"{}\"".format(input_folder, output_folder))
    elif move_mode == "M":
        print("Moving Files from \"{}\" to \"{}\"".format(input_folder, output_folder))
    elif move_mode == "T":
        print("Trashing Files from \"{}\"".format(input_folder))
    elif move_mode == "D":
        print("PERMANENTLY DELETING Files from \"{}\"".format(input_folder))

    print("") # newline since first progress_bar() will \r

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""

    for path, _, files in os.walk(os.path.abspath(input_folder)):
        files_with_valid_extension_and_start = (file for file in files if (file.endswith(file_extensions) and file.startswith(start_with)))
        for file in files_with_valid_extension_and_start:
            source_file_path = os.path.abspath(path+"/"+file)
            try:
                if move_mode == "C":
                    copy2(source_file_path, output_folder)
                elif move_mode == "M":
                    move(source_file_path, output_folder)
                elif move_mode == "T":
                    send2trash(source_file_path)
                elif move_mode == "D":
                    os.remove(source_file_path)
            except Error: # happens if destination path/filename combo exists already
                success: bool = move_file_error(source_file_path, output_folder, file, move_mode)
                if not success:
                    error_count += 1
            except: # unknown error
                error_count += 1
            number_of_files_processed += 1
            progress_bar(number_of_files_processed/number_of_files_total, 100)

    return error_count


def move_file_error(source_file_path, destination_folder, filename: str, move_mode: str = "C", max_retries = 100) -> None:
    """
    deals with errors in copying a file.
    it's probably just that the destination already has the filename

    returns True for resolved or False for not resolved
    """
    assert (move_mode in ["C", "M"]), "move_mode invalid for error handling"
    assert (os.path.exists(source_file_path)), "source_file_path does not exist"
    assert (type(filename) == str), "filename was not string"

    if not os.path.exists(destination_folder):
        try:
            os.mkdir(destination_folder)
        except:
            assert (False), "destination folder didn't exist and couldn't be created"

    error_is_filename_conflict = os.path.exists(os.path.abspath(destination_folder+"/"+filename))

    if error_is_filename_conflict:
        # check if files are the same
        source_file_stats = os.stat(source_file_path)
        destination_file_stats = os.stat(os.path.abspath(destination_folder+"/"+filename))

        source_size = source_file_stats.st_size
        destination_size = destination_file_stats.st_size
        is_size_identical = (source_size == destination_size)

        # this code will assume that if you have a filename conflict where both files have:
        # - same size in bytes
        # that these are the same file. It's unlikely that they aren't, that, well, I'm not going to run a checksum
        # I previously had date checks but they were unreliable and resulted in duplicated files

        if (not is_size_identical):
            # at least one thing is different about the files, so they are not the same file
            # in that case, simply rename the file (and retry with a different number until success)
            for retry_count in range(max_retries):
                # retry up to 100 times to copy file with new filename
                new_filename_parts = filename.split(".")
                new_filename = ".".join(new_filename_parts[:-1]) + " ({})".format(retry_count) + "." + new_filename_parts[-1]
                try:
                    if move_mode == "C":
                        copy2(source_file_path, os.path.abspath(destination_folder+"/"+new_filename))
                    else:
                        move(source_file_path, os.path.abspath(destination_folder+"/"+new_filename))
                    return True # error was resolved
                except Error: # happens if destination path/filename combo exists already
                    pass

        else:
            # the files are identical in size, modification date, and creation date,
            # we assume they are the same file, so we can move the source file to trash
            # and keep the destination file where it is, if set to move. if copy then do nothing.
            if move_mode == "M":
                send2trash(source_file_path)
            return True # error was resolved
    
    else:
        # for now I don't know what else the error could be
        return False # error was not resolved

    return False # in case somehow code gets to here, error was clearly not resolved


def remove_comment_from_input(input: str) -> str:
    return input.split("#")[0]

def string_to_tuple(string: str, delimiter: str = " ") -> tuple[str]:
    """
    if string is empty, will return empty tuple,
    else will do what you expect it to do
    """
    if string == "":
        return tuple()
    else:
        return tuple(string.split(delimiter))


def main() -> None:
    read_config_or_not = input("Read config file (Y) or enter properties manually (anything else)?:\n").split("#")[0]
    print("")

    if read_config_or_not.upper() == "Y":
        with open("Copy-All_Files-From-Folder_V0.3.config", "r") as file:
            file_lines: list[str] = file.readlines()
        # done reading file

        # assign each line to correct variable while removing comments
        get_file_extensions_or_run_program = remove_comment_from_input(file_lines[0])
        input_folder = remove_comment_from_input(file_lines[1])

        if get_file_extensions_or_run_program.upper() == "Y":
            print(get_file_extensions(input_folder))

        else:
            output_folder = remove_comment_from_input(file_lines[2])
            file_extensions = string_to_tuple(remove_comment_from_input(file_lines[3]), " ") # remove comment and convert to tuple[str]
            file_starts = string_to_tuple(remove_comment_from_input(file_lines[4]), " ") # remove comment and convert to tuple[str]
            move_mode = remove_comment_from_input(file_lines[5])

            print("\n\n" + str(move_files(input_folder, output_folder, file_extensions, file_starts, move_mode)) + " errors")

    else: # user inputs, not config file
        get_file_extensions_or_run_program = remove_comment_from_input(input("Get File Extensions? (Y, anything else runs program normally):\n"))
        print("")

        if get_file_extensions_or_run_program.upper() == "Y":
            print(get_file_extensions(remove_comment_from_input(input("Folder to get file extensions list from:\n"))))

        else:
            input_folder = remove_comment_from_input(input("Input Folder (all files with specified extensions will be copied/moved from this folder and all subfolders):\n"))
            print("")
            output_folder = remove_comment_from_input(input("Output Folder (all files will be copied/moved into this directory, not necessary for trash or deletion):\n"))
            print("")
            file_extensions = remove_comment_from_input(input("File Extensions to copy/move (space separated, ex: '.jpeg .mp4 .txt'):\n"))
            print("")
            file_starts = remove_comment_from_input(input("File beginnings to copy/move (space separated, ex: '.'):\n"))
            print("")
            move_mode = remove_comment_from_input(input("Copy, Move, Trash, or permanently Delete? (M for Move, C for Copy, T for Trash, D for PERMANENTLY DELETE):\n"))
            print("")

            file_extensions = string_to_tuple(file_extensions, " ")
            file_starts = string_to_tuple(file_starts, " ")

            print("\n\n" + str(move_files(input_folder, output_folder, file_extensions, file_starts, move_mode)) + " errors")

    return None


if __name__ == "__main__":
    main()

