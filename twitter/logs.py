import configparser
import logging
import os
from logging.handlers import RotatingFileHandler

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")
logger = logging.getLogger('social')


def configure_logging():
    # set log level
    logger.setLevel(logging.DEBUG)

    # log file path
    path = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(path, 'logs', config.get('LOG', 'FILE'))

    # create file handler
    handler = RotatingFileHandler(log_path, mode='a',
                                  maxBytes=50 * 1024 * 1024, backupCount=int(config.get('LOG', 'BACKUP')))
    # set log level
    handler.setLevel(logging.DEBUG)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(thread)d - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)



