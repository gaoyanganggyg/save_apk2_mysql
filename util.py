# -*- coding: utf-8 -*-
import logging
import logging.handlers as handlers
import config
import MySQLdb


def init_log(base_path, logger_name):
    path = base_path + "/" + logger_name
    logger = logging.getLogger(logger_name)
    fileh = handlers.RotatingFileHandler(path, "a", 1024*1024, 5)
    fileh.setFormatter(logging.Formatter(config.LOGGER_FORMATTER))
    logger.addHandler(fileh)
    logger.setLevel(logging.DEBUG)
    return logger


def init_db_conn():
    try:
        return MySQLdb.Connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, passwd=config.DB_PWD,
                               db=config.DB_DB, charset=config.DB_CHARSET)
    except Exception as e:
        raise e

