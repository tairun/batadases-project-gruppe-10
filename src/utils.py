#!/usr/bin/env python3


from functools import wraps
from time import time


def bufcount(filename: str) -> int:
    """
    Return the line count of a file.
    Source: https://gist.github.com/zed/0ac760859e614cd03652
    """
    f = open(filename, mode="r", encoding="utf8")
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read  # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    f.close()

    return lines


def timer(func):
    """A simple decorator that times the duration of a function's execution."""
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            print(f"Total execution time: {end_ if end_ > 0 else 0} ms")
    return _time_it
