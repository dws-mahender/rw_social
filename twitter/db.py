import configparser
import logging
import os
from pymongo import MongoClient
from redis import StrictRedis

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")
logger = logging.getLogger('social')


def connect_redis():
    """
    Creates a redis connection.
    :return: redis connection
    """
    try:
        r = StrictRedis(host=config.get('REDIS', 'HOST'), port=int(config.get('REDIS', 'PORT')),
                        password=config.get('REDIS', 'PWD'), decode_responses=True)
        r.ping()
        logger.info('Connected Redis !')
        return r

    except Exception as e:
        logger.error('Error in connecting to redis : {}'.format(e))


def connect_mongo():
    """
    Creates a mongo connection
    :return: mongo connection to a particular db
    """
    try:
        client = MongoClient('mongodb://' + config.get('MONGO', 'user') + ':' + config.get('MONGO', 'pwd') + '@' +
                             config.get('MONGO', 'host') + '/' + config.get('MONGO', 'authDB')
                             + '?readPreference=primary')
        # client = MongoClient("mongodb://localhost:27017/")
        connection = client[config.get('MONGO', 'db')]
        return connection
    except Exception as e:
        logger.error('Error in connecting to mongo : {}'.format(e))



