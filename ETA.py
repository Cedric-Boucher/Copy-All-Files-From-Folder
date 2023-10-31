from time import time


class ETA:
    def __init__(self):
        self.__start_time = time()
        self.__time_remaining = -1 # not defined yet
        return None

    def __update(self, progress: float) -> None:
        """
        updates self.time_remaining
        progress should be float [0, 1]
        """
        assert (type(progress) in (float, int)), "progress was not float or int"
        assert (0 <= progress), "progress was not >= 0"
        time_delta = (time() - self.__start_time)
        if progress != 0:
            estimated_total_time = (time_delta / progress)
        else:
            estimated_total_time = (3.15 * 10**7) # 1 year
        if progress > 1:
            progress = 1
        self.__time_remaining = (estimated_total_time - time_delta)
        return None

    def get_time_remaining(self, progress) -> float:
        """
        getter for self.time_remaining
        """
        self.__update(progress)
        return self.__time_remaining
    
    def get_time_since_init(self) -> float:
        return time() - self.__start_time

# make unit tests for ETA
