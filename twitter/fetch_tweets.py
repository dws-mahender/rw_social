import configparser
from datetime import datetime, timedelta
from json import load, dumps, loads
import logging
import os
from tweepy.error import TweepError
from time import sleep
from tweepy import Cursor
from db import connect_redis, connect_mongo
from twitter_client import get_twitter_client
import logs
import pika


config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")

# create logger
logger = logging.getLogger('social')

# configure logging
logs.configure_logging()


def feed_saver_new_keyword_tweets(channel, tweets):
    try:
        channel.queue_declare(queue='save_twitter_kwds', durable=True)

        # Quality of Service
        channel.basic_qos(prefetch_count=1)

        channel.basic_publish(exchange='',
                              routing_key='save_twitter_kwds',
                              body=dumps(tweets, default=str),
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
    except Exception as e:
        print('Saver feeder exception : ', e)
        print('From feed_saver_new_keyword_tweet')


def process_page(page, kw, pg_no):
    """

    :param page:
    :param kw:
    :param pg_no:
    :return:
    """
    i = 0
    result = dict()
    tweets = list()
    tweet_id = 0
    for tweet in page:
        i += 1
        if not pg_no and i == 1:  # first page first tweet
            result['status'] = 200
            result['since_id'] = tweet.id
        elif pg_no:  # after first page
            result['status'] = 201

        tweet_id = tweet.id
        user = tweet.user.screen_name
        link = "https://www.twitter.com/{}/status/{}".format(user, tweet_id)
        title = "{} tweeted about {}".format(user, kw['kwd'])
        add_data = dumps({'tweet_id': tweet.id})
        # kw_id, src, added, time, title, text, link, add_data, author
        data = (kw['k_id'], 1, tweet.created_at, datetime.utcnow(), title, tweet.text, link, add_data, user)
        tweets.append(data)
    result['k_id'] = kw['k_id']
    result['tweets'] = tweets
    return result, tweet_id


def fetch_tweets(kwd, since_id, channel, redis_conf):
    """

    :param kwd:
    :param since_id:
    :param channel:
    :param redis_conf:
    :return:
    """
    page_index = 0
    r = redis_conf['cursor']
    key = redis_conf['key']
    api, credential_id = get_twitter_client(r, key)
    tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since_id=since_id).pages()
    retry = 0
    t_id = 0

    while True:
        try:
            tweets, t_id = process_page(tweets_cursor.next(), kwd, page_index)
            feed_saver_new_keyword_tweets(channel, tweets)
            page_index += 1
            sleep(2)
        except StopIteration:
            if page_index == 0:
                # No Tweets Found
                data = {'status': 404, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            else:
                # last packet for this kwd so that saver can update scheduled_on
                data = {'status': 202, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            logger.info('Tweets Finished..')
            print('Tweets Finished..')
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            return True
        except TweepError as error:
            if error.api_code == 429:
                logger.error("Rate limit reached for credential with Id {} ".format(credential_id))

                # Change credential & lpush current credential id
                r.lpush(key, credential_id)

                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since_id=t_id).pages()
                continue

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error('Exception occurred for keyword {}. Exception : {}'.format(kwd['kwd'], e))
            retry += 1
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            if retry <= 1:
                print("Retrying...")
                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since_id=t_id).pages()
                continue
            return False


def fetch_tweets_new(kwd, channel, redis_conf):
    """

    :param kwd:
    :param channel:
    :param redis_conf:
    :return:
    """
    page_index = 0
    r = redis_conf['cursor']
    key = redis_conf['key']
    api, credential_id = get_twitter_client(r, key)
    if not api:
        logger.error("Credential {} is failing authentication".format(credential_id))

    since = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')  # 24 hr
    tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since=since).pages()
    retry = 0
    t_id = 0
    while True:
        try:
            tweets, t_id = process_page(tweets_cursor.next(), kwd, page_index)
            feed_saver_new_keyword_tweets(channel, tweets)
            page_index += 1
            sleep(2)
        except StopIteration:
            if page_index == 0:
                # No Tweets Found
                data = {'status': 404, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            else:
                # last packet for this kwd so that saver can update scheduled_on
                data = {'status': 202, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            logger.info('Tweets Finished..')
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            return True

        except TweepError as error:
            if error.api_code == 429:
                logger.error("Rate limit reached for credential with Id {} ".format(credential_id))

                # Change credential & lpush current credential id
                r.lpush(key, credential_id)

                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since=since, max_id=t_id).pages()
                continue

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error('Exception occurred for keyword {}. Exception : {}'.format(kwd['kwd'], e))
            retry += 1
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            if retry <= 1:
                print("Retrying...")
                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=kwd['kwd'], count=100, since=since, max_id=t_id).pages()
                continue
            return False

