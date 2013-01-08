import datetime

def parse_to_datetime(created_at_string):
    return datetime.datetime.strptime(created_at_string, "%Y-%m-%d %H:%M:%S.%f")

def parse_to_datetime_from_git(git_date_str):
    return datetime.datetime.strptime(git_date_str, "%a %b %d %H:%M:%S %Y +0000")
