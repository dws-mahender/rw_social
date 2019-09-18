import configparser
from datetime import datetime, timedelta
from json import dumps
import logging
import os
from tweepy.error import TweepError
from time import sleep
from tweepy import Cursor
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
        channel.queue_declare(queue=config.get('SAVER', 'QUEUE'), durable=True)

        # Quality of Service
        channel.basic_qos(prefetch_count=1)

        channel.basic_publish(exchange='',
                              routing_key=config.get('SAVER', 'QUEUE'),
                              body=dumps(tweets, default=str),
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
    except Exception as e:
        logger.error(f"Saver feeder exception : : {e}")
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
        data = (kw['k_id'], 1, tweet.created_at, datetime.utcnow(), title, tweet.full_text, link, add_data, user)
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
    r = redis_conf['cursor']
    key = redis_conf['key']

    api, credential_id = get_twitter_client(r, key)
    if not api:
        logger.info(f"{credential_id} failed ...using another one ...")
        api, credential_id = get_twitter_client(r, key)

    keyword = kwd['kwd']
    keyword = f'"{keyword} "' + config.get('FETCHER', 'FILTER')

    page_remaining = int(config.get('FETCHER', 'PAGE_LIMIT'))
    tweets_cursor = Cursor(api.search, q=keyword, count=100, since_id=since_id,
                           tweet_mode='extended').pages(page_remaining)
    page_index = 0
    retry = 0
    t_id = 0
    _sleep = 0
    sleep_delay = int(config.get('FETCHER', 'SLEEP'))
    while True:
        try:
            print(kwd, page_index)
            tweets, t_id = process_page(tweets_cursor.next(), kwd, page_index)
            feed_saver_new_keyword_tweets(channel, tweets)
            page_index += 1
            page_remaining = int(config.get('FETCHER', 'PAGE_LIMIT')) - page_index
            # sleep(1)

        except StopIteration:
            if page_index == 0:
                # No Tweets Found
                data = {'status': 404, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            else:
                # last packet for this kwd so that saver can update scheduled_on
                data = {'status': 202, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)

            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            return True

        except TweepError as error:
            logger.error(f"Tweepy Exception occurred for credential id {credential_id} : {error}")
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            retry += 1
            if retry <= 2:
                logger.info(f"Retrying for keyword {kwd['kwd']}")
                _sleep += sleep_delay
                sleep(_sleep)
                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=keyword, count=100,
                                       since_id=since_id, max_id=t_id, tweet_mode='extended').pages(page_remaining)
                continue
            # finally after retries
            data = {'status': 500, 'k_id': kwd['k_id']}
            feed_saver_new_keyword_tweets(channel, data)
            return False

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error(f"Exception occurred for keyword {kwd['kwd']}. Exception : {e}")
            retry += 1
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            if retry <= 2:
                _sleep += sleep_delay
                logger.info(f"Retrying for keyword {kwd['kwd']}")
                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=keyword, count=100,
                                       since_id=since_id, max_id=t_id, tweet_mode='extended').pages(page_remaining)
                continue

            data = {'status': 500, 'k_id': kwd['k_id']}
            feed_saver_new_keyword_tweets(channel, data)
            return False


def fetch_tweets_new(kwd, channel, redis_conf):
    """

    :param kwd:
    :param channel:
    :param redis_conf:
    :return:
    """
    r = redis_conf['cursor']
    key = redis_conf['key']
    api, credential_id = get_twitter_client(r, key)
    if not api:
        logger.info(f"{credential_id} failed ...using another one ...")
        api, credential_id = get_twitter_client(r, key)

    upto = int(config.get('FETCHER', 'DAYS'))
    since = datetime.strftime(datetime.now() - timedelta(upto), '%Y-%m-%d')

    keyword = kwd['kwd']
    keyword = f'"{keyword} "' + config.get('FETCHER', 'FILTER')

    page_remaining = int(config.get('FETCHER', 'PAGE_LIMIT'))
    tweets_cursor = Cursor(api.search, q=keyword, count=100,
                           since=since, tweet_mode='extended').pages(page_remaining)
    page_index = 0
    retry = 0
    t_id = 0
    _sleep = 0
    sleep_delay = int(config.get('FETCHER', 'SLEEP'))

    while True:
        try:
            print(kwd, page_index)
            tweets, t_id = process_page(tweets_cursor.next(), kwd, page_index)
            feed_saver_new_keyword_tweets(channel, tweets)
            page_index += 1
            page_remaining = int(config.get('FETCHER', 'PAGE_LIMIT')) - page_index

        except StopIteration:
            if page_index == 0:
                # No Tweets Found
                data = {'status': 404, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)
            else:
                # last packet for this kwd so that saver can update scheduled_on
                data = {'status': 202, 'k_id': kwd['k_id']}
                feed_saver_new_keyword_tweets(channel, data)

            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            return True

        except TweepError as error:

            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            retry += 1
            logger.error(f"Tweepy Exception occurred for credential id {credential_id} : {error}")

            if retry <= 2:

                _sleep += sleep_delay
                logger.info(f"Retrying for keyword {kwd['kwd']}")
                sleep(_sleep)

                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=keyword, count=100,
                                       since=since, max_id=t_id, tweet_mode='extended').pages(page_remaining)
                continue

            data = {'status': 500, 'k_id': kwd['k_id']}
            feed_saver_new_keyword_tweets(channel, data)
            return False

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error(f"Exception occurred for keyword {kwd['kwd']}. Exception : {e}")
            retry += 1
            # Change credential & lpush current credential id
            r.lpush(key, credential_id)
            if retry <= 2:

                _sleep += sleep_delay
                logger.info(f"Retrying for keyword {kwd['kwd']}")
                sleep(_sleep)

                api, credential_id = get_twitter_client(r, key)
                tweets_cursor = Cursor(api.search, q=keyword, count=100,
                                       since=since, max_id=t_id, tweet_mode='extended').pages(page_remaining)
                continue

            data = {'status': 500, 'k_id': kwd['k_id']}
            feed_saver_new_keyword_tweets(channel, data)
            return False

