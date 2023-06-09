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
        assert (type(progress) == float), "progress was not float"
        assert (0 <= progress <= 1), "progress was not [0, 1]"
        time_delta = (time() - self.__start_time)
        estimated_total_time = (time_delta / progress)
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

