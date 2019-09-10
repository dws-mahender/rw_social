import configparser
from json import load
from tweepy import OAuthHandler, API
from tweepy.error import TweepError
import os
import logging

logger = logging.getLogger('social')
config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")


def authenticate_credential(c):
    """
    Setup Twitter API Client.
    :param c: credentials for tweepy.OAuthHandler
    :return: tweepy.API object
    """

    # Setup tweepy to authenticate with Twitter credentials:
    auth = OAuthHandler(c['CONSUMER_KEY'], c['CONSUMER_SECRET'])
    auth.set_access_token(c['ACCESS_TOKEN'], c['ACCESS_SECRET'])

    # Create the api to connect to twitter with your credentials
    # api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
    api = API(auth, compression=True)

    try:
        api.verify_credentials()
    except TweepError as e:
        # if e.api_code == 32:
        logger.error("Authentication error : {}".format(e))
        # send alert
        return False

    except Exception as e:
        logger.error("Error during authentication of credential . Error : ", e)
        # send alert
        return False

    return api


def get_twitter_client(r, key):
    """
    Credentials for twitter client, usually called to change credentials when RateLimit exceeds.
    :param r: Redis cursor.
    :param key: Redis key in which different credential for tweepy.OAuthHandler are maintained.
    :return: tuple (tweepy.API object, credential Id for which API object is returned)
    """
    cred_id = r.rpop(key)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), config.get('CREDENTIAL', 'FILE'))) as fp:
        c = load(fp)
    credential = c['twitter'][cred_id]

    # Authenticate Credential
    api = authenticate_credential(credential)
    if not api:
        logger.error("Credential {} is failing authentication".format(cred_id))
        return False
    return api, cred_id


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


def clear_credentials(r, key):
    r.delete(key)
    print('cleared credentials')

