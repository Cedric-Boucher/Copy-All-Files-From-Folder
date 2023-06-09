def seconds_to_time(secs: float) -> str:
    """
    converts seconds into string:
    {:2d} years, {:3d} days, {:2d} hours, {:2d} minutes, {:6.2f} seconds
    """
    seconds = secs%60 # remainder after removing seconds above 60
    minutes = int(secs//60%60) # one minute is 60 seconds, so mod the time by seconds in a minute, then take the remainder after removing minutes above 60
    hours = int(secs//3600%24) # one hour is 3600 seconds, so mod the time by seconds in an hour, then take the remainder after removing hours above 24
    days = int(secs//86400%365) # one day is 86400 seconds, so mod the time by seconds in a day, then take the remainder after removing days above 365
    years = int(secs//31536000) # one year is 31536000 seconds, so mod the time by seconds in a year

    output_string = "{:2d} years, {:3d} days, {:2d} hours, {:2d} minutes, {:5.2f} seconds".format(years, days, hours, minutes, seconds)

    return output_string

