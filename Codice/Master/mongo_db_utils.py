import pymongo, json
import tweet_processing as tp
import resource_initializer as ri
from pymongo import InsertOne


def preprocess_all_tweets(ShardAddress, ShardPort):

    #get mongos data
    with open('resources/setting.json') as json_file:
        setting_data = json.load(json_file)

    # instantiate the mongos client
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master['TwitterEmotions']

    slang_dict = findSlang(db)
    emoticon_list = findEmoticon(db)
    stop_word_list = findStopWord(db)

    # connect to local shard
    client_shard = pymongo.MongoClient(ShardAddress, ShardPort)
    col = client_shard["TwitterEmotions"].Tweet
    tweet_list = findTweet(col)

    count = 0

    #process each tweet
    bulk = col.initialize_unordered_bulk_op()

    for tweet in tweet_list:
        words, emoticons, hashtags = tp.process_tweet(tweet[0], emoticon_list, slang_dict, stop_word_list)

        bulk.find({'_id': tweet[2]}).update({'$set': {
             "Words": words, "Emoticon": emoticons, "Hashtag": hashtags}})

        count = count + 1
        if (count % 10000) == 0:
            bulk.execute()
            bulk = col.initialize_unordered_bulk_op()
            print("Processati " + str(count) + "/" + str(len(tweet_list)))

    if (count % 10000) != 0:
        bulk.execute()


def findStopWord(db):
    col = db.StopWord
    stop_word_list = []
    for stop_word in col.find():
        stop_word_list.append(stop_word["Word"])
    return stop_word_list


def findEmoticon(db):
    col = db.Emoticon
    emoticon_dict = {}
    for emoticon in col.find():
        if emoticon["Code"] not in emoticon_dict:
            emoticon_dict[emoticon["Code"]] = emoticon["Polarity"]
    return emoticon_dict


def findNegativeWord(db):
    col = db.NegativeWord
    negative_word_list = []
    for negative_word in col.find():
        negative_word_list.append(negative_word["Word"])
    return negative_word_list


def findSlang(db):
    col = db.Slang
    slang_dict = {}
    for slang in col.find():
        if slang["Slang"] not in slang_dict:
            slang_dict[slang["Slang"]] = slang["Traduction"]
    return slang_dict


def findTweet(col):

    tweet_list = []
    for tweet in col.find({}):
        tweet_list.append([tweet["Text"], tweet["Emotion"], tweet['_id']])

    return tweet_list





def initialise_cluster(setting_data, skip_tweets = False):

    mongos_data = setting_data["Mongos_client"]

    client_master = pymongo.MongoClient(mongos_data["Address"],
                                        mongos_data["Port"])
    db = client_master["TwitterEmotions"]

    print('Inserting neg words')
    load_negative_word(db)

    print('Inserting slang')
    load_slang(db)

    print('Inserting stop words')
    load_stopwords(db)

    print('Inserting emoji and emoticons')
    load_emojii_emoticon(db)

    if not skip_tweets:
        print('Inserting tweets')
        load_tweet(db)

    print('Inserting word resources')
    load_resources(db)




def load_emojii_emoticon(db):

    col = db.Emoticon
    col.delete_many({})
    col.insert_many(ri.load_emojii_emoticon())


def load_stopwords(db):
    stopwords_documents = []
    for stop_word in ri.stop_words:
        stopwords_documents.append({"Word": stop_word})

    col = db.StopWord
    col.delete_many({})
    col.insert_many(stopwords_documents)


def load_slang(db):
    slang_documents = []
    for key in ri.slang:
        slang_documents.append({"Slang": key, "Traduction": ri.slang[key]})

    col = db.Slang
    col.delete_many({})
    col.insert_many(slang_documents)


def load_negative_word(db):
    negative_word = []
    for line in ri.load_negative_word():
        negative_word.append({"Word": line.strip()})

    col = db.NegativeWord
    col.delete_many({})
    col.insert_many(negative_word)


def load_tweet(db):

    col = db.Tweet
    col.delete_many({})
    print("tweets cancelled, begin to insert...")

    tweets = []

    count = 1

    for tweet in ri.load_tweet():
        tweets.append({"Text": tweet[0], "Emotion": tweet[1]})

        if count % 80000 == 0:
            col.insert_many(tweets, ordered=False)
            tweets = []

            print("Number of tweets inserted:", count)

        count += 1

    if count % 80000 == 0:
        col.insert_many(tweets, ordered=False)


def load_resources(db):

    col = db.WordCount
    col.delete_many({})

    word_counts = []
    for row in ri.load_emotions():

        emotion = row[0]

        word_counts.append({"_id" : {"Emotion" : emotion, 'Word': row[1] }, 'Count': 0, "FlagEmoSN": row[2],
                      "FlagNRC": row[3], "FlagSentisense": row[4]})

    #documents = []
    #for key in word_counts:
    #    documents.append({"Emotion": key, "Words": word_counts[key]})

    col.insert_many(word_counts)




