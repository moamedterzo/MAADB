from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher

import pymongo
from bson.json_util import dumps


client = None

def preprocess_tweets(s):

    #main(client)
    return dumps("ok")

@Request.application
def main_application(request):

    dispatcher["preprocess_tweets"] = preprocess_tweets

    response = JSONRPCResponseManager.handle(request.data, dispatcher)
    return Response(response.json, mimetype='application/json')


def start_slave_application(service_port, dbaddress, dbport):

    global client
    client = pymongo.MongoClient(dbaddress, dbport)

    run_simple('localhost', service_port, main_application)