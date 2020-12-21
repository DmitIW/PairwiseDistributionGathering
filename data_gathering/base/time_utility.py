import time
import math
import datetime


def current_time() -> int:
    return math.floor(time.time())


def current_unix_minute() -> int:
    return current_time() // 60


def current_time_str() -> str:
    return datetime.datetime.now().strftime("%A, %B %d, %I:%M %p")
