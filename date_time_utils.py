from datetime import datetime, timedelta
from pytz import timezone
from dateutil import parser as date_parser


# takes timestamp string and splits it into (dayOfWeek, Hour) tuple
def to_day_of_week_hour(string_time_stamp):
    if string_time_stamp == '':
        return '', ''
    else:
        dow = date_parser.parse(string_time_stamp).weekday()
        hour = date_parser.parse(string_time_stamp).hour
        return dow, hour


# takes timestamp string and return the date string
def to_date(string_time_stamp):
    if string_time_stamp == '':
        return ''
    else:
        date = date_parser.parse(string_time_stamp).date().strftime('%Y-%m-%d')
        return date


# Generate hours
def hours_generator(min_hour, max_hour):
    h = []
    for i in range(min_hour, max_hour):
        string_time_stamp = datetime(2016, 7, 6, i, 4, 2).strftime('%Y-%m-%d %H:%M:%S')
        h.append(date_parser.parse(string_time_stamp).hour)
    return h


def convert_int_to_hour_string(hour_int):
    return datetime(2016, 7, 6, hour_int, 4, 2).strftime('%H')


def day_of_week_generator():
    day_of_week = []
    for i in range(1, 8):
        string_time_stamp = datetime(2016, 7, i, 4, 4, 2).strftime('%Y-%m-%d %H:%M:%S')
        day_of_week.append(date_parser.parse(string_time_stamp).weekday())
    return day_of_week


# Creates a date string from n days back from now
def get_date_n_days_ago(daysAgo):
    date_N_days_ago = datetime.now() - timedelta(days=daysAgo)
    return date_N_days_ago.date().strftime('%Y-%m-%d')


# Get current time by time zone
def get_current_time_by_timezone(time_zone):
    utc_now = datetime.utcnow()
    local_tz = timezone(time_zone)
    return local_tz.localize(utc_now)
