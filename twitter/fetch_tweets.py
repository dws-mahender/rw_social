import configparser
from datetime import datetime, timedelta
from json import load, dumps, loads
import logging
import os
from tweepy.error import TweepError
from time import sleep
from tweepy import Cursor
from social.twitter.db import connect_redis, connect_mongo
from social.twitter.twitter_client import get_twitter_client
from social.twitter import logs

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")

# create logger
logger = logging.getLogger('social')


def load_credentials(r, key):
    """

    :param r:
    :param key:
    :return:
    """
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), config.get('CREDENTIAL', 'FILE'))) as fp:
        c = load(fp)
    credentials = c['twitter'].keys()
    [r.lpush(key, credential) for credential in credentials]
    logger.info('Credentials loaded in redis')


def make_keywords_queue(r, collection, r_key):
    """

    :param r:
    :param collection:
    :param r_key:
    :return:
    """
    # get keywords
    kwds_list = collection.find({"active": 1}, {"_id": 1, "kw": 1, "chkst": 1})

    # make redis keywords queue
    for kwd in kwds_list:
        kwd_data = dumps({
            'kwd': kwd['kw'],
            'chkst': 1 if kwd['chkst'] else 0,
            'since_id': kwd['since_id'] if 'since_id' in kwd else 0
        })

        r.lpush(r_key, kwd_data)
    logger.info('Pushed all data to redis')


def process_page(page, kwd, pg_no):
    """

    :param page:
    :param kwd:
    :param pg_no:
    :return:
    """
    i = 0
    for tweet in page:
        i += 1
        if pg_no == 0 and i == 1:
            # update t_id
            pass

        tweet_id = tweet.id
        text = tweet.text
        src = 106 if "http" in text else 102
        user = tweet.user.screen_name
        link = "https://www.twitter.com/{}/status/{}".format(user, tweet_id)
        data = {
            'dt': datetime.utcnow(),
            'tweet_date': tweet.created_at,
            'tweet_id': tweet.id,
            'title': "{} tweeted about {}".format(user, kwd),
            'desc': tweet.text,
            'chkst': 1,
            'src': src,
            'domain': 'twitter.com',
            'link': link
        }
        print(data)


def fetch_tweets(kwd, since_id):
    """

    :param kwd:
    :param since_id:
    :return:
    """
    page_index = 0
    api, credential_id = get_twitter_client(redis_cursor, r_cred)
    tweets = Cursor(api.search, q=kwd, count=100, since_id=since_id).pages()

    while True:
        try:
            process_page(tweets.next(), kwd, page_index)
            page_index += 1
            sleep(3)
        except StopIteration:
            print('Tweets Finished..')
            return True
        except TweepError as error:
            if error.api_code == 429:
                logger.error("Rate limit reached for credential with Id {} ".format(credential_id))

                # Change credential & lpush current credential id
                redis_cursor.lpush(r_cred, credential_id)

                api, credential_id = get_twitter_client(redis_cursor, r_cred)
                continue

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error('Exception occurred for keyword {}. Exception : {}'.format(kwd, e))
            return False


def fetch_tweets_new(kwd):
    """

    :param kwd:
    :return:
    """
    page_index = 0
    api, credential_id = get_twitter_client(redis_cursor, r_cred)
    if not api:
        logger.error("Credential {} is failing authentication".format(credential_id))

    today = datetime.today().strftime('%Y-%m-%d')
    since = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')  # 24 hr
    tweets = Cursor(api.search, q=kwd, count=100, since=since, until=today).pages()

    while True:
        try:
            process_page(tweets.next(), kwd, page_index)
            page_index += 1
            sleep(2)
        except StopIteration:
            logger.info('Tweets Finished..')
            return True

        except TweepError as error:
            if error.api_code == 429:
                logger.error("Rate limit reached for credential with Id {} ".format(credential_id))

                # Change credential & lpush current credential id
                redis_cursor.lpush(r_cred, credential_id)

                api, credential_id = get_twitter_client(redis_cursor, r_cred)
                continue

        except Exception as e:
            # push keyword in queue & maintain log
            logger.error('Exception occurred for keyword {}. Exception : {}'.format(kwd, e))
            return False


def process_job(kwd_list):
    """

    :param kwd_list:
    :return:
    """
    while True:
        raw_keyword = redis_cursor.brpop(kwd_list, timeout=300)
        if not raw_keyword:
            logger.error('Redis Keyword list empty')
            # ALERT
        else:
            keyword = loads(raw_keyword[1])
            kwd = keyword['kwd']
            if keyword['chkst'] == -1:  # new keyword
                response = fetch_tweets_new(kwd)
                if not response:
                    # lpush keyword again and continue
                    redis_cursor.lpush(kwd_list, raw_keyword)
                    continue
            else:
                # already processed keyword
                response = fetch_tweets(kwd, since_id=keyword['since_id'])
                if not response:
                    # lpush keyword again and continue
                    redis_cursor.lpush(kwd_list, raw_keyword)
                    continue


def main():

    # Get Keywords and push them into redis
    db = connect_mongo()
    social_keywords = db['dev_social_keywords']

    # push all keywords in redis queue
    make_keywords_queue(redis_cursor, social_keywords, r_kwds)

    # Load Twitter API Authentication credential Ids
    load_credentials(redis_cursor, r_cred)

    # process keywords
    process_job(kwd_list=r_kwds)


if __name__ == "__main__":
    # configure logging
    logs.configure_logging()

    # connect to redis and get a cursor
    redis_cursor = connect_redis()

    # SET REDIS KEYS
    # this queue maintains all ips for which health report is to be judged
    r_kwds = config.get('REDIS', 'TWITTER_KWDS')
    # contains credentials list
    r_cred = config.get('REDIS', 'CREDENTIALS_LIST')

    main()




