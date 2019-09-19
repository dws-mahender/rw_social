import configparser
from db import connect_mongo
from json import dumps
import logging
import logs
import os
import pika
import time

# create logger
logger = logging.getLogger('social')

# configure logging
logs.configure_logging()


def get_pika_connection():
    credentials = pika.PlainCredentials(config.get('RABBITMQ', 'USER'), config.get('RABBITMQ', 'PWD'))
    host = config.get('RABBITMQ', 'HOST')
    parameters = pika.ConnectionParameters(host, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    return connection


def schedule_kwds():
    db = connect_mongo()
    social_keywords = db['request_log']
    current_time = int(time.time())
    # print(current_time)
    scheduled_kwds = social_keywords.find({"src_id": 1, "new": 5, "status": 1,
                                           "scheduled_on": {"$lte": current_time}, "queued": 0},
                                          {"scheduled_on": 1, "kw": 1, "k_id": 1, "since_id": 1, "queued": 1})
    #  "queued": {"$exists": False}} so that new kwds can not be duplicated
    new_kwds = social_keywords.find({"src_id": 1, "new": 1, "status": 1, "queued": {"$exists": False}}, {"kw": 1, "k_id": 1})
    connection = get_pika_connection()

    # scheduled kwds
    channel_scheduled_kwds = connection.channel()
    channel_scheduled_kwds.queue_declare(queue=config.get('CONSUMER', 'SCHEDULED_Q'), durable=True)
    # Quality of Service
    channel_scheduled_kwds.basic_qos(prefetch_count=int(config.get('CONSUMER', 'PREFETCH')))

    # New kwds
    channel_new_kwds = connection.channel()
    channel_new_kwds.queue_declare(queue=config.get('CONSUMER', 'RUN_NOW_Q'), durable=True)
    # Quality of Service
    channel_new_kwds.basic_qos(prefetch_count=int(config.get('CONSUMER', 'PREFETCH')))

    queued_kwds = list()
    for kwd in scheduled_kwds:
        kwd_data = dumps({
            'kwd': kwd['kw'],
            'k_id': kwd['k_id'],
            'since_id': kwd['since_id']
        })
        channel_scheduled_kwds.basic_publish(exchange='',
                                             routing_key=config.get('CONSUMER', 'SCHEDULED_Q'),
                                             body=kwd_data,
                                             properties=pika.BasicProperties(
                                                  delivery_mode=2,  # make message persistent
                                             ))
        queued_kwds.append(kwd['k_id'])

    for kwd in new_kwds:
        kwd_data = dumps({
            'kwd': kwd['kw'],
            'k_id': kwd['k_id']
        })
        channel_new_kwds.basic_publish(exchange='',
                                       routing_key=config.get('CONSUMER', 'RUN_NOW_Q'),
                                       body=kwd_data,
                                       properties=pika.BasicProperties(
                                          delivery_mode=2,  # make message persistent
                                       ))
        queued_kwds.append(kwd['k_id'])

    if queued_kwds:
        n = social_keywords.update_many({"src_id": 1, "k_id": {"$in": queued_kwds}}, {"$set": {"queued": 1}})
        print(f"Total queued kwds {n.modified_count}")
        print(" [x] Sent !")
    else:
        print("No queued keywords")

    channel_scheduled_kwds.close()
    channel_new_kwds.close()
    connection.close()
    time.sleep(int(config.get('SCHEDULER', 'SLEEP')))


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")

    while True:
        schedule_kwds()
