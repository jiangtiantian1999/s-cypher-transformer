import traceback
from transformer.exceptions.datetime_exception import DatetimeException


def main():
    try:
        pass
    except DatetimeException as e:
        print(traceback.format_exc())
        print(e.__class__.__name__, ":", e)


if __name__ == '__main__':
    main()
