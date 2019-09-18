from db import connect_pg, connect_mongo, connect_es
import configparser
import hashlib
import os
import pika
import multiprocessing
from json import loads
from datetime import datetime
from psycopg2 import extras
from time import time


def bulk_insert(cursor, postgres, chunks):
    # data = list()
    # for chunk in chunks:
    #     # hash_id : md5 of  keyword_id, src_id, link, title & text.
    #     _hash = str(chunk[0]) + '1' + chunk[6] + chunk[4] + chunk[5]
    #     hash_id = hashlib.md5(_hash.encode('utf-8')).hexdigest()
    #     # check if hash already exists
    #     hash_exists = elastic_search.exists(index='post_hash', doc_type='posts', id=hash_id)
    #     if hash_exists:
    #         # ignore this chunk and do not append it to bulk query.
    #         continue
    #     else:
    #         # create es index and append this chunk for bulk query.
    #         es_body = {
    #             'k_id': chunk[0],
    #             'title': chunk[4],
    #             'link': chunk[6],
    #             'desc': chunk[5]
    #         }
    #         res = elastic_search.index(index='post_hash', doc_type='posts', id=hash_id, body=es_body)
    #         data.append({
    #             'kw_id': chunk[0],
    #             'src': chunk[1],
    #             'added': chunk[2],
    #             'time': chunk[3],
    #             'title': chunk[4],
    #             'text': chunk[5],
    #             'link': chunk[6],
    #             'add_data': chunk[7],
    #             'author': chunk[8]
    #         })
    data = [{
                'kw_id': chunk[0],
                'src': chunk[1],
                'added': chunk[2],
                'time': chunk[3],
                'title': chunk[4],
                'text': chunk[5],
                'link': chunk[6],
                'add_data': chunk[7],
                'author': chunk[8]
            } for chunk in chunks]
    query = cursor.mogrify("INSERT INTO {} ({}) VALUES {} RETURNING {}".format(
        'app_post',
        ', '.join(data[0].keys()),
        ', '.join(['%s'] * len(data)),
        'id, added, author'
    ), [tuple(v.values()) for v in data])
    cursor.execute(query)
    postgres.commit()
    post_ids = cursor.fetchall()
    return post_ids


def save_tweets():
    print(multiprocessing.current_process(), "Started ... ")

    q = config.get('SAVER', 'QUEUE')
    delay = int(config.get('SAVER', 'DELAY'))
    normal_delay = int(config.get('SAVER', 'NORMAL_DELAY'))

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=int(config.get('SAVER', 'PREFETCH')))
    channel.queue_declare(queue=q, durable=True)

    #  postgres
    postgres = connect_pg()
    cur = postgres.cursor()
    # mongo
    db = connect_mongo()
    social_keywords = db['request_log']

    # elastic-search
    # es = connect_es()

    try:
        while True:
            for method_frame, properties, body in channel.consume(q):
                kwd = loads(body)
                users = social_keywords.find({"src_id": 1, "k_id": kwd['k_id']}, {"users": 1, "delay": 1, "frequency": 1})
                users_list = users[0]['users']
                total_delay = users[0]['delay'] * users[0]['frequency']
                # Delay should not exceed more than 24 hr
                total_delay = total_delay if total_delay < delay else delay
                post_ids = list()

                if kwd['status'] == 200:  # Tweets for first page
                    # Update since id
                    social_keywords.update_one({"src_id": 1,
                                                "k_id": kwd['k_id']},
                                               {"$set": {"since_id": kwd['since_id'], "new": 5}})
                    # Update tweets
                    post_ids = bulk_insert(cur, postgres, kwd['tweets'])
                elif kwd['status'] == 201:  # Tweets after first page
                    post_ids = bulk_insert(cur, postgres, kwd['tweets'])
                elif kwd['status'] == 202:  # All  tweets for this kwd finished
                    scheduled_on = int(time()) + normal_delay
                    social_keywords.update_one({"src_id": 1,
                                                "k_id": kwd['k_id']},
                                               {"$set": {"scheduled_on": scheduled_on, "queued": 0, "new": 5}}
                                               )
                elif kwd['status'] == 404:  # No Tweets found for the keyword
                    scheduled_on = int(time()) + total_delay
                    social_keywords.update_one({"src_id": 1,
                                                "k_id": kwd['k_id']},
                                               {"$set": {"scheduled_on": scheduled_on, "queued": 0, "new": 5},
                                                "$mul": {"frequency": 2}}
                                               )
                elif kwd['status'] == 500:  # Exception occurred
                    scheduled_on = int(time()) + normal_delay
                    social_keywords.update_one({"src_id": 1,
                                                "k_id": kwd['k_id']},
                                               {"$set": {"scheduled_on": scheduled_on, "queued": 0, "new": 5}}
                                               )
                if post_ids:
                    data = list()
                    for user in users_list:
                        for post in post_ids:
                            data.append((user['user_id'], kwd['k_id'], user['p_id'], post[0], post[1], post[2], 1,
                                         datetime.utcnow(), 0, 0, 1))

                    query = """Insert into app_userposts (user_id,kw_id,project_id,post_id,added,author,src,time,read,fav,status)
                                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); """
                    extras.execute_batch(cur, query, data)
                    postgres.commit()
                # Acknowledge the message
                channel.basic_ack(method_frame.delivery_tag)

    except KeyboardInterrupt:
        # Close the channel ,pika connection and db connection
        print("Closing connection ....")
        channel.close()
        connection.close()
        postgres.close()
        cur.close()
        db.close()
        social_keywords.close()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(os.path.dirname(os.path.abspath(__file__)) + "/../config.ini")

    workers = int(config.get('SAVER', 'WORKERS'))
    pool = multiprocessing.Pool(processes=workers)
    for i in range(0, workers):
        pool.apply_async(save_tweets)  # also has callback option

    # Stay alive
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print(' [*] Exiting...')
        pool.terminate()
        pool.join()




