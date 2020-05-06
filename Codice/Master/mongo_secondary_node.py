from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpc import JSONRPCResponseManager, dispatcher

import mongo_db_utils as mu

DBAddress = None
DBPort = None

flag_preprocessing_running = False


@Request.application
def main_application(request):

    dispatcher["preprocess_tweets"] = preprocess_tweets
    dispatcher["is_preprocess_complete"] = is_preprocess_complete

    response = JSONRPCResponseManager.handle(request.data, dispatcher)
    return Response(response.json, mimetype='application/json')


def is_preprocess_complete():
    if mu.running_threads_preprocessing_tweets == 0:
        return "ok"
    else:
        return "wait"

def preprocess_tweets():

    print("Start to preprocess all shard tweets...")
    mu.preprocess_all_tweets(DBAddress, DBPort, False)
    print("Preprocessing started...")
    return "wait"


def start_secondary_node(service_port, db_address, db_port, service_binding="0.0.0.0"):

    global DBPort
    DBPort = db_port

    global DBAddress
    DBAddress = db_address

    run_simple(service_binding, service_port, main_application)