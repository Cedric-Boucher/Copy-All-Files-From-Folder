from shutil import copy2, move, Error
from send2trash import send2trash
import os
from progress_bar import progress_bar
from file_folder_getters import *
from concurrent.futures import ThreadPoolExecutor
from filecmp import cmp as compare_files
from time import time
import argparse


def parse_inputs() -> tuple[bool, str, str, list[str], list[str], str]:
    """
    takes care of parsing the command line arguments passed to the program

    returns tuple:
    (get_file_extensions: bool, input_folder: str, output_folder: str, file_extensions: list[str], file_beginnings: list[str], operation: str)
    """
    parser = argparse.ArgumentParser(description="Does various things related to file handling and moving")
    parser.add_argument("--get_file_extensions", "-gfe", type=bool, nargs="?", help="bool, True for getting file extensions", choices=(True, False), default=False)
    parser.add_argument("--input_folder", "-if", type=str, nargs="?", required=True, help="str, path to the input folder for processing")
    parser.add_argument("--output_folder", "-of", type=str, nargs="?", help="str, path to the output folder for processing")
    parser.add_argument("--file_extensions", "-fe", type=str, nargs="*", help="str, list the file extensions you want to limit processing to", default=[])
    parser.add_argument("--file_beginnings", "-fb", type=str, nargs="*", help="str, list the file beginnings you want to limit processing to", default=[])
    parser.add_argument("--operation", "-op", type=str, nargs="?", choices=("C", "M", "T", "D"), help="str, file operation to perform (Copy, Move, Trash, Delete)")
    parser.add_argument("--confirm_permanent_delete", "-cpd", type=bool, nargs="?", help="bool, required to be True to permanently delete any files", choices=(True, False), default=False)
    args = parser.parse_args()

    return (args.get_file_extensions, args.input_folder, args.output_folder, args.file_extensions, args.file_beginnings, args.operation, args.confirm_permanent_delete)


def move_files(input_folder, output_folder = None, file_extensions: tuple[str] = (), start_with: tuple[str] = (), move_mode: str = "C", keep_folder_structure: bool = True) -> list[tuple]:
    """
    move_mode can be either "M" for move, "C" for copy, "T" for trash, "D" for permanently delete

    output_folder should be defined for move_mode C or M, but is unused for T or D

    if file_extensions/start_with is empty tuple then all file extensions will be copied/moved

    if keep_folder_structure is False, all files in input folder and its subfolders will be dumped into the output folder,
    this only applies for move_mode in ["C", "M"]

    returns the errors
    """
    assert (move_mode in ["C", "M", "T", "D"]), "move_mode was not one of the options"
    assert (type(file_extensions) == tuple), "file_extensions was not a tuple"
    assert (type(start_with) == tuple), "start_with was not a tuple"
    assert (os.path.exists(input_folder)), "input_folder does not exist"
    assert (type(keep_folder_structure) == bool), "keep_folder_structure was not bool"

    same_drive_input_output = (os.path.splitdrive(input_folder)[0] == os.path.splitdrive(output_folder)[0])
    if move_mode == "C" or (move_mode == "M" and not same_drive_input_output):
        # copy / move time is mainly based on raw MB/s throughput of drives
        rate_units = "MB"
    else:
        # basically just changing a few bytes in the filesystem per file,
        # move time is based on seek time and is constant regardless of file size
        rate_units = "files"


    if move_mode in ["C", "M"]:
        if not os.path.exists(output_folder):
            try:
                os.makedirs(output_folder)
            except:
                assert (False), "destination folder didn't exist and couldn't be created"

    number_of_files_total = get_num_files_in_folder(os.path.abspath(input_folder), file_extensions=file_extensions, start_with=start_with)
    number_of_files_processed = 0
    error_counts = [0 for _ in range(9999)] # I hope that I never have over 9999 possible error codes

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
        confirm_delete = (input("Are you sure you want to continue? (Y, anything else to cancel)\n") == "Y")
        if not confirm_delete:
            return [] # since output should be list of tuples, return empty list

    print("") # newline since first progress_bar() will \r

    if len(file_extensions) == 0:
        file_extensions = "" # all strings end with ""

    if len(start_with) == 0:
        start_with = "" # all strings start with ""


    progress_bar_object = progress_bar(100, rate_units=rate_units) # created progress bar object
    progress = 0

    with ThreadPoolExecutor() as executor:
        for path, _, files in os.walk(os.path.abspath(input_folder)):
            unit_output = executor.submit(move_files_unit_processor, files, path, input_folder, output_folder, start_with, file_extensions, move_mode, keep_folder_structure)
            (new_error_counts, new_number_of_files_processed, number_of_failed_files, new_total_processed_size, failed_files_size) = unit_output.result()
            number_of_files_total -= number_of_failed_files
            total_size -= failed_files_size
            for i in range(len(error_counts)):
                error_counts[i] += new_error_counts[i]
            number_of_files_processed += new_number_of_files_processed
            total_processed_size += new_total_processed_size

            # update progress
            if move_mode == "C" or (move_mode == "M" and not same_drive_input_output):
                # copy / move time is mainly based on raw MB/s throughput of drives
                try:
                    progress = total_processed_size / total_size
                except ZeroDivisionError:
                    progress = 1 / 2**32
                rate_progress = total_processed_size / (10**6)
            else:
                # basically just changing a few bytes in the filesystem per file,
                # move time is based on seek time and is constant regardless of file size
                progress = number_of_files_processed/number_of_files_total
                rate_progress = number_of_files_processed

            progress_bar_object.print_progress_bar(progress, rate_progress)

    # process error_counts to only return what errors did happen:
    error_return: list[tuple] = list()
    for error_number in range(len(error_counts)):
        if error_counts[error_number] > 0:
            error_return.append((error_number, error_counts[error_number]))

    return error_return


def move_files_unit_processor(files: list[str], path: str, input_folder, output_folder, start_with: tuple[str], end_with: tuple[str], move_mode: str, keep_folder_structure: bool):
    """
    multithreaded unit processor for move files
    do not use on its own
    """
    total_processed_size = 0
    error_counts = [0 for _ in range(9999)] # I hope that I never have over 9999 possible error codes
    number_of_files_processed = 0
    number_of_failed_files = 0
    failed_files_size = 0

    files_with_valid_extension_and_start = (file for file in files if (file.endswith(end_with) and file.startswith(start_with)))
    for file in files_with_valid_extension_and_start:
        success = (-1, "") # reset to assume no problems happen
        source_file_path = os.path.abspath(path+"/"+file)
        current_filesize = os.stat(source_file_path)[6] # bytes filesize
        total_processed_size += current_filesize

        if keep_folder_structure:
            relative_output_path = path.removeprefix(input_folder) # the subfolder structure inside of input_folder
            output_folder_path = os.path.abspath(output_folder + "/" + relative_output_path) # copy that subfolder structure to output
        else:
            output_folder_path = output_folder

        if move_mode in ["C", "M"]:
            output_folder_exists = os.path.exists(output_folder_path)
            if not output_folder_exists:
                try:
                    os.makedirs(output_folder_path)
                except:
                    assert (False), "destination folder didn't exist and couldn't be created"
            output_file_exists = os.path.exists(os.path.abspath(output_folder_path+"/"+file))

        try:
            if move_mode == "C":
                if not output_file_exists:
                    copy2(source_file_path, output_folder_path)
                else:
                    # if file already exists, check if it's the same file, etc
                    success = move_file_error(source_file_path, output_folder_path, file, move_mode)
                    error_counts[success[0]] += 1
            elif move_mode == "M":
                if not output_file_exists:
                    move(source_file_path, output_folder_path)
                else:
                    # if file already exists, you can trash this copy
                    success = move_file_error(source_file_path, output_folder_path, file, move_mode)
                    error_counts[success[0]] += 1
            elif move_mode == "T":
                send2trash(source_file_path)
            elif move_mode == "D":
                os.remove(source_file_path)
        except Error: # this shouldn't happen, and the line below will not be able to fix it
            success = move_file_error(source_file_path, output_folder_path, file, move_mode)
            error_counts[success[0]] += 1
        except FileNotFoundError: # file was deleted, renamed or moved before it could be processed
            error_counts[6] += 1
        except: # unknown error
            error_counts[5] += 1
        number_of_files_processed += 1

        # if there was a failure, update the progress accordingly
        if success[0] in (0, 1, 3, 5):
            number_of_files_processed -= 1
            number_of_failed_files += 1
            total_processed_size -= current_filesize
            failed_files_size += current_filesize

    return (error_counts, number_of_files_processed, number_of_failed_files, total_processed_size, failed_files_size)


def move_file_error(source_file_path, destination_folder, filename: str, move_mode: str = "C", max_retries = 100) -> tuple[int, str]:
    """
    deals with errors in copying a file.
    it's probably just that the destination already has the filename

    returns a pair of error number and accompanying string to explain the error
    """
    assert (move_mode in ["C", "M"]), "move_mode invalid for error handling"
    assert (os.path.exists(source_file_path)), "source_file_path does not exist"
    assert (type(filename) == str), "filename was not string"

    errors: list[tuple[int, str]] = [(0, "File already existed and nothing was changed"),
                                     (1, "File already existed and extra copy was trashed"),
                                     (2, "File was renamed and copied/moved"),
                                     (3, "Couldn't find a filename that worked, gave up"),
                                     (4, "File was renamed to resolve conflict"),
                                     (5, "Error couldn't be resolved"),
                                     (6, "File could not be found")]

    if not os.path.exists(destination_folder):
        try:
            os.makedirs(destination_folder)
        except:
            assert (False), "destination folder didn't exist and couldn't be created"

    error_is_filename_conflict = os.path.exists(os.path.abspath(destination_folder+"/"+filename))

    if error_is_filename_conflict:
        # check if files are the same
        files_are_identical = compare_files(source_file_path, os.path.abspath(destination_folder+"/"+filename), shallow = False)

        if files_are_identical:
            # assumed to be the same file, original can be safely moved to trash
            if move_mode == "M":
                send2trash(source_file_path)
                return errors[1]
            # if move mode was copy then do nothing
            return errors[0]

        for retry_count in range(max_retries):
            # retry up to 100 times to copy file with new filename
            new_filename_parts = filename.split(".")
            new_filename = ".".join(new_filename_parts[:-1]) + " ({})".format(retry_count) + "." + new_filename_parts[-1]
            destination_exists = os.path.exists(os.path.abspath(destination_folder+"/"+new_filename))

            if not destination_exists:
                break # new_filename can be used

            files_are_identical = compare_files(source_file_path, os.path.abspath(destination_folder+"/"+filename), shallow = False)

            if files_are_identical:
                if move_mode == "M":
                    send2trash(source_file_path)
                    return errors[1]
                # if move mode was copy then do nothing
                return errors[0]
        
        if destination_exists and not files_are_identical:
            # this means we went through all the retry attempts
            # and couldn't find somewhere to put source file,
            # so we gave up
            return errors[3]

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
            return errors[4] # error was resolved
        except Error:
            # couldn't resolve the issue for some reason
            return errors[5]

    else: # if error was not filename conflict
        # for now I don't know what else the error could be
        return errors[5] # error was not resolved


def main() -> None:
    start_time = time()
    (get_file_extensions_or_run_program, input_folder, output_folder, file_extensions, file_starts, move_mode, permanent_delete_confirmed) = parse_inputs()
    assert (os.path.exists(input_folder)), "input folder does not exist"

    if get_file_extensions_or_run_program: # True means get file extensions
        [print(extension, end=" ") for extension in get_file_extensions(input_folder)]
        print("") # add a newline after the list

    else:
        assert (move_mode in ("C", "M", "T", "D")), "operation type invalid or not given"
        assert (move_mode != "D" or permanent_delete_confirmed), "permanent deletion must be confirmed with --confirm_permanent_delete or -cpd argument set to True"
        if move_mode not in ("T", "D"):
            assert (os.path.exists(output_folder)), "output folder does not exist"
        file_extensions = tuple(file_extensions)
        file_starts = tuple(file_starts)

        print("\n\nerrors: " + str(move_files(input_folder, output_folder, file_extensions, file_starts, move_mode)))

    print("{} seconds to run".format(time() - start_time))
    return None


if __name__ == "__main__":
    main()
