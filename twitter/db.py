import configparser
# from elasticsearch import Elasticsearch
import logging
import os
import psycopg2
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
        return False


def connect_mongo():
    """
    Creates a mongo connection
    :return: mongo connection to a particular db
    """
    try:
        client = MongoClient('mongodb://' + config.get('MONGO', 'USER') + ':' + config.get('MONGO', 'PWD') + '@' +
                             config.get('MONGO', 'HOST') + '/' + config.get('MONGO', 'AUTHDB')
                             + '?readPreference=primary', connect=False)
        # client = MongoClient("mongodb://localhost:27017/")
        connection = client[config.get('MONGO', 'DB')]
        return connection
    except Exception as e:
        logger.error('Error in connecting to mongo : {}'.format(e))
        return False


def connect_pg():
    try:
        connection = psycopg2.connect(user=config.get('POSTGRES', 'USER'),
                                      password=config.get('POSTGRES', 'PWD'),
                                      host=config.get('POSTGRES', 'HOST'),
                                      port=config.get('POSTGRES', 'PORT'),
                                      database=config.get('POSTGRES', 'DB'))

        return connection
    except psycopg2.DatabaseError as e:
        print(e)
        return e.pgerror


# def connect_es():
#     try:
#         es = Elasticsearch([{'host': '192.99.247.142', 'port': 9200}])
#         if es.ping():
#             return es
#         else:
#             logger.error(f"Unable to ping elastic search : {e}")
#             return False
#
#     except Exception as e:
#         logger.error(f"Error in connecting to elastic search : {e}")
#         return False

