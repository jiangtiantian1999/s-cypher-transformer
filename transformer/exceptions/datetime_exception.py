class DatetimeException(Exception):
    def __int__(self, message):
        super().__init__(message)
        self.message = message


class FormatError(DatetimeException):
    def __int__(self, message):
        super().__init__(message)
        self.message = message


class TypeError(DatetimeException):
    def __int__(self, message):
        super().__init__(message)
        self.message = message


class ValueError(DatetimeException):
    def __int__(self, message):
        super().__init__(message)
        self.message = message


class TimeZoneError(DatetimeException):
    def __int__(self, message):
        super().__init__(message)
        self.message = message
