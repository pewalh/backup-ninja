import numpy as np


def pretty_size(size_bytes:int):
    if size_bytes == 0:
        return '0 B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = np.floor(np.log(size_bytes) / np.log(1024))
    p = np.power(1024, i)
    s = round(size_bytes / p, 2)
    return f'{s} {size_name[int(i)]}'


def pretty_time(seconds:float):
    if seconds < 60:
        return f'{seconds:.2f} s'
    elif seconds < 60*60:
        return f'{seconds/60:.2f} min'
    elif seconds < 60*60*24:
        return f'{seconds/60/60:.2f} h'
    else:
        return f'{seconds/60/60/24:.2f} d'

