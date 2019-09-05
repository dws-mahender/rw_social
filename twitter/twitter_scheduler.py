from datetime import datetime
from db import connect_mongo
from json import dumps
import pika
import time


def schedule_kwds():
    db = connect_mongo()
    social_keywords = db['request_log']
    current_time = time.time()
    kwds = social_keywords.find({"src_id": 1, "queued": 0, "scheduled_on": {"$lte": current_time}},
                                {"scheduled_on": 1, "kw": 1, "k_id": 1, "since_id": 1, "queued": 1})

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    queued_kwds = list()
    for kwd in kwds:
        if 'since_id' in kwd:

            channel.queue_declare(queue='twitter_kwds', durable=True)

            # Quality of Service
            channel.basic_qos(prefetch_count=1)

            kwd_data = dumps({
                'kwd': kwd['kw'],
                'k_id': kwd['k_id'],
                'since_id': kwd['since_id'],
                'old': True
            })
            channel.basic_publish(exchange='',
                                  routing_key='twitter_kwds',
                                  body=kwd_data,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,  # make message persistent
                                  ))
            queued_kwds.append(kwd['k_id'])
        else:
            print("New kwd scheduled for 24 hr calculation :", kwd["kw"])

            channel.queue_declare(queue='new_twitter_kwds', durable=True)

            # Quality of Service
            channel.basic_qos(prefetch_count=1)

            kwd_data = dumps({
                'kwd': kwd['kw'],
                'k_id': kwd['k_id']
            })
            channel.basic_publish(exchange='',
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
    connection.close()
    time.sleep(120)


if __name__ == '__main__':
    while True:
        schedule_kwds()
