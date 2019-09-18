import configparser
from db import connect_redis
from fetch_tweets import fetch_tweets_new, fetch_tweets
from json import loads
import logging
import logs
import multiprocessing
import os
import pika
from twitter_client import load_credentials, clear_credentials


# create logger
logger = logging.getLogger('social')

# configure logging
logs.configure_logging()


def get_tweets(ch, method, properties, body):
    kwd = loads(body)
    print(" [x] %r received %r" % (multiprocessing.current_process(), kwd,))

    if 'since_id' in kwd:
        response = fetch_tweets(kwd=kwd, since_id=kwd['since_id'], channel=ch, redis_conf=redis_config)
    else:
        response = fetch_tweets_new(kwd=kwd, channel=ch, redis_conf=redis_config)

    if not response:
        # push it to queue again
        logger.info(f'False response from fetch tweets for {kwd}')

    # acknowledgment from the worker, once we're done with a task.
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_scheduled_kwds():
    q = config.get('CONSUMER', 'SCHEDULED_Q')
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', credentials=credentials,
                                           heartbeat=heartbeat, blocked_connection_timeout=timeout)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.basic_qos(prefetch_count=int(config.get('CONSUMER', 'PREFETCH')))

    channel.queue_declare(queue=q, durable=True)

    channel.basic_consume(queue=q, on_message_callback=get_tweets)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()
        channel.close()
        # channel.stop_consuming()


def consume_new_kwds():
    q = config.get('CONSUMER', 'RUN_NOW_Q')
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', credentials=credentials,
                                           heartbeat=heartbeat, blocked_connection_timeout=timeout)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.basic_qos(prefetch_count=int(config.get('CONSUMER', 'PREFETCH')))

    channel.queue_declare(queue=q, durable=True)

    channel.basic_consume(queue=q, on_message_callback=get_tweets)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()
        channel.close()
        # channel.stop_consuming()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")

    # connect to redis and get a cursor
    redis_cursor = connect_redis()

    # SET REDIS KEYS
    r_kwds = config.get('REDIS', 'TWITTER_KWDS')
    # contains credentials list
    r_cred = config.get('REDIS', 'CREDENTIALS_LIST')

    # Load Twitter API Authentication credential Ids
    load_credentials(redis_cursor, r_cred)

    redis_config = {'cursor': redis_cursor, 'key': r_cred}

    heartbeat = int(config.get('CONSUMER', 'HEARTBEAT'))
    timeout = int(config.get('CONSUMER', 'TIMEOUT'))
    workers = int(config.get('CONSUMER', 'WORKERS'))

    pool = multiprocessing.Pool(processes=workers)
    for i in range(0, workers):
        # pool.apply_async(consume_scheduled_kwds)  # also has callback option
        pool.apply_async(consume_new_kwds)
    # Stay alive
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print(' [*] Exiting...')
        pool.terminate()
        pool.join()
        clear_credentials(redis_cursor, r_cred)
