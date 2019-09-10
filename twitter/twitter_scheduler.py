from datetime import datetime
from db import connect_mongo
from json import dumps
import pika
import time


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
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

    # scheduled kwds
    channel_scheduled_kwds = connection.channel()
    channel_scheduled_kwds.queue_declare(queue='twitter_kwds', durable=True)
    # Quality of Service
    channel_scheduled_kwds.basic_qos(prefetch_count=1)

    # New kwds
    channel_new_kwds = connection.channel()
    channel_new_kwds.queue_declare(queue='new_twitter_kwds', durable=True)
    # Quality of Service
    channel_new_kwds.basic_qos(prefetch_count=1)

    queued_kwds = list()
    for kwd in scheduled_kwds:
        kwd_data = dumps({
            'kwd': kwd['kw'],
            'k_id': kwd['k_id'],
            'since_id': kwd['since_id']
        })
        channel_scheduled_kwds.basic_publish(exchange='',
                                             routing_key='twitter_kwds',
                                             body=kwd_data,
                                             properties=pika.BasicProperties(
                                                  delivery_mode=2,  # make message persistent
                                             ))
        queued_kwds.append(kwd['k_id'])

    for kwd in new_kwds:
        print("New kwd scheduled for 24 hr calculation :", kwd["kw"])
        kwd_data = dumps({
            'kwd': kwd['kw'],
            'k_id': kwd['k_id']
        })
        channel_new_kwds.basic_publish(exchange='',
                                       routing_key='new_twitter_kwds',
                                       body=kwd_data,
                                       properties=pika.BasicProperties(
                                          delivery_mode=2,  # make message persistent
                                       ))
        queued_kwds.append(kwd['k_id'])

    if queued_kwds:
        n = social_keywords.update_many({"src_id": 1, "k_id": {"$in": queued_kwds}}, {"$set": {"queued": 1}})
        print("Total queued kwds ", n.modified_count)
        print(" [x] Sent !")
    else:
        print("No queued keywords")

    channel_scheduled_kwds.close()
    channel_new_kwds.close()
    connection.close()
    time.sleep(120)


if __name__ == '__main__':
    while True:
        schedule_kwds()
