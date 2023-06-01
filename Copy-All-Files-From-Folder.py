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

    if len(file_extensions == 0):
        file_extensions == "" # all strings end with ""

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


def move_files(input_folder: str, output_folder: str, file_extensions: tuple[str], move_or_copy: str) -> int:
    """
    move_or_copy can be either "M" or "C"
    
    returns number of errors, prints progress
    """
    assert (move_or_copy in ["C", "M"]), "move_or_copy was not 'C' or 'M'"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"

    if move_or_copy == "C":
        print("Copying Files from {} to {}".format(input_folder, output_folder))
    else:
        print("Moving Files from {} to {}".format(input_folder, output_folder))

    print("") # newline since first progress_bar() will \r

    number_of_files_total = get_num_files_with_file_extension(os.path.abspath(input_folder), file_extensions)
    number_of_files_processed = 0
    errors = 0

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


def move_file_error(source_file_path: str, destination_folder: str, filename: str, move_or_copy: str, max_retries = 100) -> None:
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

        source_modification_time = source_file_stats.st_mtime
        destination_modification_time = destination_file_stats.st_mtime
        is_modification_time_identical = (source_modification_time == destination_modification_time)

        source_creation_time = source_file_stats.st_ctime
        destination_creation_time = destination_file_stats.st_ctime
        is_creation_time_identical = (source_creation_time == destination_creation_time)

        # this code will assume that if you have a filename conflict where both files have:
        # - modification time,
        # - creation time, AND
        # - size in bytes
        # that these are the same file. It's so insanely unlikely that they aren't, that, well, I'm not going to run a checksum

        if (not is_size_identical) or (not is_modification_time_identical) or (not is_creation_time_identical):
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
            # and keep the destination file where it is
            send2trash(source_file_path)
    
    else:
        # for now I don't know what else the error could be
        return False # error was not resolved

    return False # in case somehow code gets to here, error was clearly not resolved


def main() -> None:
    get_file_extensions_or_run_program = input("Get File Extensions? (Y, anything else runs program normally):\n")
    print("")
    if get_file_extensions_or_run_program.upper() == "Y":
        print(get_file_extensions(input("Folder to get file extensions list from:\n")))

    else:
        input_folder = input("Input Folder (all files with specified extensions will be copied/moved from this folder and all subfolders):\n")
        print("")

        output_folder = input("Output Folder (all files will be copied/moved into this directory):\n")
        print("")

        file_extensions = input("File Extensions to copy/move (space separated, ex: '.jpeg .mp4 .txt'):\n")
        print("")

        move_or_copy = input("Copy or Move? (M for Move, C for Copy):\n")
        print("")

        file_extensions = tuple(file_extensions.split(" "))

        print("\n\n" + str(move_files(input_folder, output_folder, file_extensions, move_or_copy)) + " errors")

    return None


if __name__ == "__main__":
    main()

