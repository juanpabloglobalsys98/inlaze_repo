import logging


class FilterBool(logging.Filter):
    def __init__(self, value=None):
        super(FilterBool, self).__init__()
        self._value = value

    def filter(self, record):
        return self._value
