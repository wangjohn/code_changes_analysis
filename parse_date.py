import datetime

def parse_to_datetime(created_at_string):
    return datetime.datetime.strptime(created_at_string, "%Y-%m-%d %H:%M:%S.%f")
