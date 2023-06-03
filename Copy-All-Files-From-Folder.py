"""
VERSION 0.1

compatible with config files versioned V0.1

"""


from shutil import copy2, move, Error
from send2trash import send2trash
import os
import time


def get_file_extensions(path: str) -> tuple[str]:
    """
    returns a tuple of all file extensions is a folder and subfolders
    """
    file_extensions = list()

    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            filename_parts = file.split(".")
            file_extension = "."+filename_parts[-1]
            if file_extension not in file_extensions:
                file_extensions.append(file_extension)

    return tuple(file_extensions)


def get_num_files_with_file_extension(path: str, file_extensions: tuple[str] = (), print_stats_every_x_seconds = 1) -> int:
    """
    Counts the number of files in a directory using os.walk
    set print_stats_every_x_seconds to -1 to never print
    if file_extensions is an empty tuple, will not check file extensions,
    and count all files
    """
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    num_files = 0
    t = time.time()
    if print_stats_every_x_seconds != -1:
        print("\nChecking number of files for path "+str(path)+"...\n")
    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.endswith(file_extensions):
                num_files += 1
        if time.time() - t >= print_stats_every_x_seconds and print_stats_every_x_seconds != -1:
            print("\r{} files...".format(num_files), end="")
            t = time.time()

    return num_files


def get_num_files_that_start_with(path: str, start_with: tuple[str], print_stats_every_x_seconds = 1) -> int:
    """
    Counts the number of files in a directory using os.walk
    set print_stats_every_x_seconds to -1 to never print
    """
    assert (type(start_with) == tuple), "start_with was not a tuple"
    assert (len(start_with) > 0), "start_with was empty tuple"

    num_files = 0
    t = time.time()
    if print_stats_every_x_seconds != -1:
        print("\nChecking number of files for path "+str(path)+"...\n")
    for _, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.startswith(start_with):
                num_files += 1
        if time.time() - t >= print_stats_every_x_seconds and print_stats_every_x_seconds != -1:
            print("\r{} files...".format(num_files), end="")
            t = time.time()

    return num_files


def progress_bar(progress: float, length: int, start_string = "~<{", end_string = "}>~", fill_char = "/", empty_char = "-", overwrite_previous_bar = True, with_percentage = True) -> None:
    """
    progress is float [0, 1]
    length is character count length of progress bar (not including start and end strings)
    no newline is printed after the progress bar
    """
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


def move_files(input_folder: str, output_folder: str, file_extensions: tuple[str], move_or_copy: str = "C") -> int:
    """
    move_or_copy can be either "M" or "C"

    if file_extensions is empty tuple then all file extensions will be copied/moved
    
    returns number of errors, prints progress
    """
    assert (move_or_copy in ["C", "M"]), "move_or_copy was not 'C' or 'M'"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"

    number_of_files_total = get_num_files_with_file_extension(os.path.abspath(input_folder), file_extensions)
    number_of_files_processed = 0
    errors = 0

    if move_or_copy == "C":
        print("Copying Files from {} to {}".format(input_folder, output_folder))
    else:
        print("Moving Files from {} to {}".format(input_folder, output_folder))

    print("") # newline since first progress_bar() will \r

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    for path, _, files in os.walk(os.path.abspath(input_folder)):
        files_with_valid_extension = (file for file in files if file.endswith(file_extensions))
        for file in files_with_valid_extension:
            source_file_path = os.path.abspath(path+"/"+file)
            try:
                if move_or_copy == "C":
                    copy2(source_file_path, output_folder)
                else:
                    move(source_file_path, output_folder)
            except Error: # happens if destination path/filename combo exists already
                success: bool = move_file_error(source_file_path, output_folder, file, move_or_copy)
                if not success:
                    errors += 1
            number_of_files_processed += 1
            progress_bar(number_of_files_processed/number_of_files_total, 100)

    return errors


def move_file_error(source_file_path: str, destination_folder: str, filename: str, move_or_copy: str = "C", max_retries = 100) -> None:
    """
    deals with errors in copying a file.
    it's probably just that the destination already has the filename

    returns True for resolved or False for not resolved
    """
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
                    if move_or_copy == "C":
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
            if move_or_copy == "M":
                send2trash(source_file_path)
            return True # error was resolved
    
    else:
        # for now I don't know what else the error could be
        return False # error was not resolved

    return False # in case somehow code gets to here, error was clearly not resolved


def delete_all_files_that_start_with(folder_path: str, start_with: tuple[str], do_permanent_delete: bool = False) -> int:
    """
    deletes all files in folder_path
    and all subfolders, that start with start_with string

    returns number of errors
    """
    assert (type(start_with) == tuple), "start_with was not tuple"
    assert (len(start_with) > 0), "start_with was empty tuple"

    number_of_files_total = get_num_files_that_start_with(os.path.abspath(folder_path), start_with)
    number_of_files_processed = 0
    errors = 0

    print("deleting files that start with {} from {}".format(start_with, folder_path))

    print("") # newline since first progress_bar() will \r

    for path, _, files in os.walk(os.path.abspath(folder_path)):
        files_with_valid_extension = (file for file in files if file.startswith(start_with))
        for file in files_with_valid_extension:
            source_file_path = os.path.abspath(path+"/"+file)
            try:
                if do_permanent_delete:
                    os.remove(source_file_path)
                else:
                    send2trash(source_file_path)
            except:
                errors += 1
            number_of_files_processed += 1
            progress_bar(number_of_files_processed/number_of_files_total, 100)

    return errors


def main() -> None:
    get_file_extensions_or_run_program = input("Get File Extensions? (Y, anything else runs program normally):\n").split("#")[0]
    print("")
    if get_file_extensions_or_run_program.upper() == "Y":
        print(get_file_extensions(input("Folder to get file extensions list from:\n").split("#")[0]))

    else:
        input_folder = input("Input Folder (all files with specified extensions will be copied/moved from this folder and all subfolders):\n").split("#")[0]
        print("")

        output_folder = input("Output Folder (all files will be copied/moved into this directory):\n").split("#")[0]
        print("")

        file_extensions = input("File Extensions to copy/move (space separated, ex: '.jpeg .mp4 .txt'):\n").split("#")[0]
        print("")

        move_or_copy = input("Copy or Move? (M for Move, C for Copy):\n").split("#")[0]
        print("")

        if file_extensions == "":
            file_extensions = tuple()
        else:
            file_extensions = tuple(file_extensions.split(" "))

        print("\n\n" + str(move_files(input_folder, output_folder, file_extensions, move_or_copy)) + " errors")

    return None


if __name__ == "__main__":
    main()
    #print(delete_all_files_that_start_with("K:\Downloads\Google Photo Takeout Output", tuple(["._"])))

