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
        - [x] obtaining file extensions manually without obtaining filepaths first (singlethreaded)
        - [x] obtaining file extensions manually after obtaining filepaths manually (singlethreaded)
        - [x] obtaining file extensions manually after obtaining file extensions manually (singlethreaded)
        - [x] obtaining file hashes manually without obtaining filepaths first
        - [x] obtaining file hashes manually after obtaining filepaths manually
        - [x] obtaining file hashes manually after obtaining file hashes manually
    """
    from time import time
    from copy import deepcopy
    from Filelist import Filelist

    test_folder = "/home"
    filelists = (
        Filelist(test_folder),
        Filelist(test_folder, file_extensions=(".png", ".jpeg", ".py", ".txt")),
        Filelist(test_folder, start_with=("File", "test")),
        Filelist(test_folder, file_extensions=(".png", ".jpeg", ".py", ".txt"), start_with=("File", "test")),
        Filelist(test_folder, min_filesize=4096),
        Filelist(test_folder, max_filesize=1024**2),
        Filelist(test_folder, min_filesize=4096, max_filesize=1024**2),
        Filelist(test_folder, file_extensions=(".png", ".jpeg", ".py", ".txt"), start_with=("File", "test"), min_filesize=4096, max_filesize=1024**2)
        )

    current_test = [-1, -1]
    test_objects: list[list[Filelist]] = list()
    test_times: list[list[int]] = list()

    for filelist in filelists:
        current_test[0] += 1
        current_test[1] = -1
        test_objects.append(list())
        test_times.append(list())

        # obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filepaths()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining filepaths manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filepaths()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining file extensions manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][2])) # skip getting file extensions again by using test 2 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file sizes manually after obtaining file sizes manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][5])) # skip getting file sizes again by using test 5 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filesizes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually after obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining whether folder has files manually after obtaining whether folder has files manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][8])) # skip checking for files again by using test 8 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].does_folder_have_files()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually without obtaining filepaths first (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining filepaths manually (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file extensions manually after obtaining file extensions manually (singlethreaded)
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][2])) # skip getting file extensions again by using test 2 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_file_extensions_singlethreaded()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually without obtaining filepaths first
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(filelist))
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually after obtaining filepaths manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][0])) # skip getting filepaths again by using test 0 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)

        # obtaining file hashes manually after obtaining file hashes manually
        current_test[1] += 1
        print(current_test)
        test_objects[current_test[0]].append(deepcopy(test_objects[current_test[0]][14])) # skip getting file extensions again by using test 14 from this set
        t = time()
        test_objects[current_test[0]][current_test[1]].get_filehashes()
        result_time = time() - t
        print("{:.1e} seconds\n".format(result_time))
        test_times[current_test[0]].append(result_time)



if __name__ == "__main__":
    test_Filelist()
