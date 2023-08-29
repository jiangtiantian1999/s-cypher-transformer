class FormatError(Exception):
    def __int__(self, message):
        super().__init__(message)


class TimeZoneError(Exception):
    def __int__(self, message):
        super().__init__(message)
        self.message = message

class GraphError(Exception):
    def __int__(self, message):
        super().__init__(message)
        self.message = message
