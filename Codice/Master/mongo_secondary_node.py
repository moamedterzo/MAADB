from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher

import mongo_db_utils as mu

DBAddress = None
DBPort = None


@Request.application
def main_application(request):

    dispatcher["preprocess_tweets"] = preprocess_tweets

    response = JSONRPCResponseManager.handle(request.data, dispatcher)
    return Response(response.json, mimetype='application/json')


def preprocess_tweets(s):

    print("Start to preprocess all shard tweets...")
    mu.preprocess_all_tweets(DBAddress, DBPort)
    print("Preprocess finished!")
    return "ok"


def start_secondary_node(service_port, db_address, db_port, service_address="0.0.0.0"):

    global DBPort
    DBPort = db_port

    global DBAddress
    DBAddress = db_address

    run_simple(service_address, service_port, main_application)