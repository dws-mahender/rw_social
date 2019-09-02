from flask import Flask
from flask_restful import Resource, Api, reqparse
from json import loads, dumps
import pika

app = Flask(__name__)

# Create the API
api = Api(app)


def feed_kwds(kwd_list):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='twitter_kwds', durable=True)

    # Quality of Service
    channel.basic_qos(prefetch_count=1)
    for kwd in kwd_list:
        kwd_data = dumps({
            'kwd': kwd['kwd'],
            'k_id': kwd['k_id']
        })
        channel.basic_publish(exchange='',
                              routing_key='twitter_kwds',
                              body=kwd_data,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))

    print(" [x] Sent !")

    connection.close()


class KeywordsList(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('kwds', required=True)

        # Parse the arguments into an object
        args = parser.parse_args()

        kwds_list = loads(args['kwds'])

        feed_kwds(kwds_list)
        return {'message': 'Keywords received', 'data': args}, 201


api.add_resource(KeywordsList, '/keywords')

if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)





