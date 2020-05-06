import requests
import mongo_db_utils as mu
import threading
import pymongo
from pymongo import UpdateOne

def run_twitter_analisys(setting_data):

    preprocessing(setting_data)
    print()
    print("Fine preprocessing")

    map_reduce(setting_data)
    print("Fine map reduce")


def preprocessing(setting_data):
    threads = []

    for secondary_setting_data in setting_data['MongoDB']['SecondaryNodes']:
        threads.append(threading.Thread(target=secondary_node_call,
                                        args=(secondary_setting_data['ServiceAddress'],
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

    # words
    db.WordCount.delete_many({ "FlagEmoSN": 0, "FlagNRC": 0, "FlagSentisense":0})
    pipeline = [
        {"$unwind": "$Words"},
        {"$group": {"_id": {"Emotion": "$Emotion", "Word": "$Words"}, "Count": {"$sum": 1}}}
    ]

    bulk_requests = []
    for word_count in list(db.Tweet.aggregate(pipeline, allowDiskUse=True)):
        bulk_requests.append(UpdateOne({"_id" : word_count['_id'] }, {'$set': word_count}, upsert=True))

    db.WordCount.bulk_write(bulk_requests)

    # emoticons
    db.EmoticonCount.delete_many({})
    pipeline = [
        {"$unwind": "$Emoticon"},
        {"$group": {"_id": {"emotion": "$Emotion", "emoticon": "$Emoticon"}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.emotion", "values": {"$push": {"emoticon": "$_id.emoticon", "count": "$count"}}}}
    ]

    db.EmoticonCount.insert_many(list(db.Tweet.aggregate(pipeline)))

    #hashtags
    db.HashtagCount.delete_many({})
    pipeline = [
        {"$unwind": "$Hashtag"},
        {"$group": {"_id": { "emotion": "$Emotion", "hashtag": "$Hashtag"}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.emotion", "values" : {"$push": {"hashtag": "$_id.hashtag", "count": "$count"}}}}
    ]

    db.HashtagCount.insert_many(list(db.Tweet.aggregate(pipeline)))



def primary_node_call(Address, DBPort):
    mu.preprocess_all_tweets(Address, DBPort)
    print("Preprocess on primary shard finished!")


def secondary_node_call(Address, ServicePort):

    url = "http://" + Address + ":" + str(ServicePort) + "/jsonrpc"

    payload = {
        "method": "preprocess_tweets",
        "params": [""],
        "jsonrpc": "2.0",
        "id": 0,
    }

    print("Chiamando il servizio con url:", url)
    response = requests.post(url, json=payload,timeout=15*60).json()


    if "result" not in response or response["result"] != "ok":
        print("Errore dal nodo con url:",url)
        print(response)
    else:
        print("OK dal nodo con url:",url)


