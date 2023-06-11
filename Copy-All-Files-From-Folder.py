"""
compatible with config files versioned V0.3
"""
from shutil import copy2, move, Error
from send2trash import send2trash
import os
from progress_bar import progress_bar
from file_folder_getters import *


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

    same_drive_input_output = (os.path.splitdrive(input_folder)[0] == os.path.splitdrive(output_folder)[0])
    if move_mode == "C" or (move_mode == "M" and not same_drive_input_output):
        # copy / move time is mainly based on raw MB/s throughput of drives
        rate_units = "MB"
    else:
        # basically just changing a few bytes in the filesystem per file,
        # move time is based on seek time and is constant regardless of file size
        rate_units = "files"


    progress_bar_object = progress_bar(100, rate_units=rate_units) # created progress bar object
    progress = 0

    if move_mode in ["C", "M"]:
        if not os.path.exists(output_folder):
            try:
                os.mkdir(output_folder)
            except:
                assert (False), "destination folder didn't exist and couldn't be created"

    number_of_files_total = get_num_files_in_folder(os.path.abspath(input_folder), file_extensions=file_extensions, start_with=start_with)
    number_of_files_processed = 0
    error_count = 0

    total_size = get_size_of_folder(os.path.abspath(input_folder), file_extensions=file_extensions, start_with=start_with)
    total_processed_size = 0

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
            success = True # reset to assume true if no problems happen
            source_file_path = os.path.abspath(path+"/"+file)
            total_processed_size += os.stat(source_file_path)[6] # bytes filesize
            if move_mode in ["C", "M"]:
                output_file_exists = os.path.exists(os.path.abspath(output_folder+"/"+file))
            try:
                if move_mode == "C":
                    if not output_file_exists:
                        copy2(source_file_path, output_folder)
                    else:
                        # if file already exists, check if it's the same file, etc
                        success: bool = move_file_error(source_file_path, output_folder, file, move_mode)
                        if not success:
                            error_count += 1
                elif move_mode == "M":
                    if not output_file_exists:
                        move(source_file_path, output_folder)
                    else:
                        # if file already exists, you can trash this copy
                        success: bool = move_file_error(source_file_path, output_folder, file, move_mode)
                        if not success:
                            error_count += 1
                elif move_mode == "T":
                    send2trash(source_file_path)
                elif move_mode == "D":
                    os.remove(source_file_path)
            except Error: # this shouldn't happen, and the line below is unlikely to fix it
                success: bool = move_file_error(source_file_path, output_folder, file, move_mode)
                if not success:
                    error_count += 1
            except: # unknown error
                error_count += 1
                success = False
            number_of_files_processed += 1

            # Issue #13 there is still a bug where success is true but for the if statement below we would like it to be false,
            # this will probably be fixed as a result of Issue #6 (TODO)

            # if there was a failure, update the progress accordingly
            if not success:
                number_of_files_processed -= 1
                number_of_files_total -= 1
                total_processed_size -= os.stat(source_file_path)[6]
                total_size -= os.stat(source_file_path)[6]

            if move_mode == "C" or (move_mode == "M" and not same_drive_input_output):
                # copy / move time is mainly based on raw MB/s throughput of drives
                progress = total_processed_size / total_size
                rate_progress = total_processed_size / (10**6)
            else:
                # basically just changing a few bytes in the filesystem per file,
                # move time is based on seek time and is constant regardless of file size
                progress = number_of_files_processed/number_of_files_total
                rate_progress = number_of_files_processed

            progress_bar_object.print_progress_bar(progress, rate_progress)

    return error_count


def move_file_error(source_file_path, destination_folder, filename: str, move_mode: str = "C", max_retries = 100) -> bool:
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

        if is_size_identical:
            # assumed to be the same file, original can be safely moved to trash
            if move_mode == "M":
                send2trash(source_file_path)
            # if move mode was copy then do nothing
            return True

        for retry_count in range(max_retries):
            # retry up to 100 times to copy file with new filename
            new_filename_parts = filename.split(".")
            new_filename = ".".join(new_filename_parts[:-1]) + " ({})".format(retry_count) + "." + new_filename_parts[-1]
            destination_exists = os.path.exists(os.path.abspath(destination_folder+"/"+new_filename))

            if not destination_exists:
                break # new_filename can be used

            destination_file_stats = os.stat(os.path.abspath(destination_folder+"/"+new_filename))

            source_size = source_file_stats.st_size
            destination_size = destination_file_stats.st_size
            is_size_identical = (source_size == destination_size)

            if is_size_identical:
                # assumed to be the same file, original can be safely moved to trash
                if move_mode == "M":
                    send2trash(source_file_path)
                # if move mode was copy then do nothing
                return True
        
        if destination_exists and not is_size_identical:
            # this means we went through all the retry attempts
            # and couldn't find somewhere to put source file,
            # so we gave up
            return False

        # this code will assume that if you have a filename conflict where both files have:
        # - same size in bytes
        # that these are the same file. It's unlikely that they aren't, that, well, I'm not going to run a checksum
        # I previously had date checks but they were unreliable and resulted in duplicated files

        # if we get here, then the destination did not contain a copy of this file,
        # so we use new_filename to copy/move the source file
        try:
            if move_mode == "C":
                copy2(source_file_path, os.path.abspath(destination_folder+"/"+new_filename)) # this is guaranteed not to overwrite a file
            else:
                move(source_file_path, os.path.abspath(destination_folder+"/"+new_filename)) # this is guaranteed not to overwrite a file
            return True # error was resolved
        except Error:
            # couldn't resolve the issue for some reason
            return False

    else: # if error was not filename conflict
        # for now I don't know what else the error could be
        return False # error was not resolved


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
        with open("Copy-All-Files-From-Folder_V0.3.config", "r") as file:
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

