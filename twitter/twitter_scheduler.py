from datetime import datetime
from db import connect_mongo
from json import dumps
import pika
import time


def schedule_kwds():
    db = connect_mongo()
    social_keywords = db['request_log']
    current_time = datetime.utcnow()
    kwds = social_keywords.find({"src_id": 1, "scheduled_on": {"$lte": current_time}}, {"scheduled_on": 1, "kw": 1,
                                                                                        "k_id": 1, "since_id": 1})

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='twitter_kwds', durable=True)

    # Quality of Service
    channel.basic_qos(prefetch_count=1)
    for kwd in kwds:
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
    print(" [x] Sent !")

    connection.close()
    time.sleep(120)


if __name__ == '__main__':
    while True:
        schedule_kwds()
