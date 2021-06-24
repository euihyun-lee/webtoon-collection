import json
import hashlib
import datetime
from functools import partial


WEEKDAY_REPRS = ("Mon", "Tue", "Wed", "Thr", "Fri", "Sat", "Sun")


def mysql_json_dumps():
    def default(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

    return partial(json.dumps, ensure_ascii=False, default=default)


def sha512(string):
    hashed = hashlib.new('sha512')
    string = string.encode('utf-8')
    hashed.update(string)
    return hashed.hexdigest()


def verify_weekday(string):
    weekdays = string.split(',')
    for weekday in weekdays:
        if not weekday in WEEKDAY_REPRS:
            return False
    return True


def check_and_get(json_dict, key, optional=False):
    if not optional:
        assert key in json_dict, f"{key} must be specified."
    return json_dict.get(key)
