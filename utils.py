import json
import hashlib
import datetime
from functools import partial


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
