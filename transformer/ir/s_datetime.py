import datetime


class Date:

    def __init__(self, date):
        self.date = datetime.date(date)


class Time:

    def __init__(self, time, timezone):
        self.time = datetime.time(time, tzinfo=timezone)


class LocalTime:

    def __init__(self, time):
        self.time = datetime.time(time, tzinfo=datetime.timezone.utc)


class DateTime:

    def __init__(self, datetime, timezone):
        self.datetime = datetime
        self.timezone = timezone


class LocalDateTime:

    def __init__(self, datetime, timezone):
        self.datetime = datetime
        self.timezone = timezone


class Duration:

    def __init__(self, duration):
        self.duration = duration


class Interval:

    def __init__(self, interval_from, interval_to):
        self.interval_from = interval_from
        self.interval_to = interval_to
