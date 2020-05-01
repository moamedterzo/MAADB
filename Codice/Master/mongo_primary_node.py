import requests
import mongo_db_utils as mu
import threading
from bson.code import Code
import pymongo

def run_twitter_analisys(setting_data):

    preprocessing(setting_data)
    print()
    print("Fine preprocess")

    map_reduce(setting_data)
    print("Fine map reduce")


def preprocessing(setting_data):
    threads = []

    for secondary_setting_data in setting_data['MongoDB']['SecondaryNodes']:
        threads.append(threading.Thread(target=secondary_node_call,
                                        args=(secondary_setting_data['Address'],
                                              secondary_setting_data['ServicePort'])))

    primary_setting_data = setting_data['MongoDB']['PrimaryNode']
    threads.append(threading.Thread(target=primary_node_call,
                                    args=(primary_setting_data['Address'],
                                          primary_setting_data['DBPort'])))

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def map_reduce(setting_data):
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master['TwitterEmotions']

    with open("resources/map_words.js", 'r') as file:
        map = Code(file.read())

    with open("resources/reduce_words.js", 'r') as file:
        reduce = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults")

    f = open("output/map_reduce_word.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc) + "\n")
    f.close()

    with open("resources/map_emoticon.js", 'r') as file:
        map = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults2")

    f = open("output/map_reduce_emoticon.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc) + "\n")
    f.close()

    with open("resources/map_hashtag.js", 'r') as file:
        map = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults3", )
    f = open("output/map_reduce_hashtag.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc) + "\n")
    f.close()



def primary_node_call(Address, DBPort):
    mu.preprocess_all_tweets(Address, DBPort)


def secondary_node_call(Address, ServicePort):

    url = "http://" + Address + ":" + str(ServicePort) + "/jsonrpc"

    payload = {
        "method": "preprocess_tweets",
        "params": [""],
        "jsonrpc": "2.0",
        "id": 0,
    }

    response = requests.post(url, json=payload).json()

    if "result" not in response or response["result"] != "ok":
        print("Errore dal nodo con porta: " + str(ServicePort))
        print(response)


