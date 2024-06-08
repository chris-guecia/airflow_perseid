import logging
import datetime
import sys


def logger_config(name: str) -> logging.Logger:
    current_date = datetime.datetime.today().strftime("%Y%m%d")
    log_file_name = r"" + current_date + "test.log"

    file_formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(module)s %(funcName)s %(lineno)d %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(module)s %(funcName)s %(lineno)d %(message)s"
    )

    file_handler = logging.FileHandler(log_file_name)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(console_formatter)

    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.setLevel(logging.DEBUG)

    return logger
