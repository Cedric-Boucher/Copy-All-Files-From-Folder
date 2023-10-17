from os import path
from random import randbytes
from progress_bar import progress_bar

number_of_files: int = 500000 # number of files to create
file_size: int = 4096 # number of bytes
folder_path: str = path.abspath("C:/Users/onebi/Copy-All-Files-Demo-Input/")

if input("Total size will be {} GiB, continue? (Y/N)\n".format(file_size*number_of_files/1024/1024/1024)).upper() != "Y":
    raise InterruptedError

progress = progress_bar(50, rate_units="MiB")
for file_i in range(number_of_files):
    filename: str = "test_file_{}.test".format(file_i)
    file_path: str = path.join(folder_path, filename)
    with open(file_path, "wb") as file:
        file.write(randbytes(file_size))
    progress.print_progress_bar((file_i+1)/number_of_files, file_size*(file_i+1)/1024/1024)
