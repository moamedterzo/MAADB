import requests
import threading
from time import sleep
import pymongo
from pymongo import UpdateOne
import mongo_db_utils as mu


def run_twitter_analysis(setting_data):

    print("Starting preprocessing")
    start_distributed_preprocessing(setting_data)

    print("Starting data aggregation")
    map_reduce(setting_data)

    print("Analysis completed!")


def start_distributed_preprocessing(setting_data):
    threads = []

    # si creano tanti thread quanti saranno i nodi secondari da chiamare
    for secondary_setting_data in setting_data['SecondaryNodes']:
        threads.append(threading.Thread(target=secondary_node_call,
                                        args=(secondary_setting_data['ServiceAddress'],
                                              secondary_setting_data['ServicePort'])))
        threads[-1].start()

    # si avvia il preprocessing anche per il nodo primario
    primary_setting_data = setting_data['PrimaryNode']
    mu.preprocess_all_shard_tweets(primary_setting_data['Address'], primary_setting_data['DBPort'])

    # si attende la terminazione di tutti i thread secondari
    for t in threads:
        t.join()


def map_reduce(setting_data):

    # ci si collega a mongos
    mongos_data = setting_data["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master['TwitterEmotions']

    # aggregazione parole
    db.WordCount.delete_many({ "FlagEmoSN": 0, "FlagNRC": 0, "FlagSentisense":0})
    pipeline = [
        {"$unwind": "$Words"},
        {"$group": {"_id": {"Emotion": "$Emotion", "Word": "$Words"}, "Count": {"$sum": 1}}}
    ]

    bulk_requests = []
    for word_count in list(db.Tweet.aggregate(pipeline, allowDiskUse=True)):
        bulk_requests.append(UpdateOne({"_id" : word_count['_id'] }, {'$set': word_count}, upsert=True))

    db.WordCount.bulk_write(bulk_requests)


    # aggregazione emoticons ed emoji
    db.EmojiconCount.delete_many({})

    pipeline = [
        {"$unwind": "$Emoticon"},
        {"$group": {"_id": {"emotion": "$Emotion", "emoticon": "$Emoticon"}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.emotion", "values": {"$push": {"emoticon": "$_id.emoticon", "count": "$count"}}}}
    ]
    db.EmojiconCount.insert_many(list(db.Tweet.aggregate(pipeline)))

    # aggregazione hashtags
    db.HashtagCount.delete_many({})

    pipeline = [
        {"$unwind": "$Hashtag"},
        {"$group": {"_id": { "emotion": "$Emotion", "hashtag": "$Hashtag"}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.emotion", "values" : {"$push": {"hashtag": "$_id.hashtag", "count": "$count"}}}}
    ]
    db.HashtagCount.insert_many(list(db.Tweet.aggregate(pipeline)))


def secondary_node_call(Address, ServicePort):

    service_url = "http://" + Address + ":" + str(ServicePort) + "/jsonrpc"
    print("Calling web service:", service_url)

    payload = {
        "method": "preprocess_tweets",
        "params": [],
        "jsonrpc": "2.0",
        "id": 0,
    }
    response = requests.post(service_url, json=payload).json()

    # per non incorrere nel 'freezing' del thread, si chiama il servizio web
    # ogni tot. secondi per conoscere lo stato di processamento dei tweets
    while "result" in response and response["result"] == "wait":
        sleep(15)

        payload = {
            "method": "is_preprocess_complete",
            "params": [],
            "jsonrpc": "2.0",
            "id": 0,
        }
        response = requests.post(service_url, json=payload).json()

    # si mostra il risultato del servizio web
    if "result" not in response or response["result"] != "ok":
        print("Error from node:",service_url)
        print(response)
    else:
        print("Task completed from node:",service_url)


