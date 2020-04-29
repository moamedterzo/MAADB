import pymongo
import os
from slang_emojii_emoticon_stopwords import slang, stop_words, pos_emoticons, neg_emoticons, EmojiPos, EmojiNeg, \
    OthersEmoji

client_master = None

path_tweet = '..\..\Risorse\Twitter messaggi'
path_negative_word = "..\..\Risorse\elenco-parole-che-negano-parole-successive.txt"
path_emotions = "..\..\Risorse\Sentimenti"


def load_emojii_emoticon():
    emojii_emoticon_documents = []
    for emoticon in pos_emoticons:
        emojii_emoticon_documents.append({"Code": emoticon, "Polarity": 1})
    for emoticon in neg_emoticons:
        emojii_emoticon_documents.append({"Code": emoticon, "Polarity": -1})
    for emoticon in EmojiPos:
        emojii_emoticon_documents.append({"Code": emoticon, "Polarity": 1})
    for emoticon in EmojiNeg:
        emojii_emoticon_documents.append({"Code": emoticon, "Polarity": -1})
    for emoticon in OthersEmoji:
        emojii_emoticon_documents.append({"Code": emoticon, "Polarity": 0})

    db = client_master['TwitterEmotions']
    col = db.Emoticon
    col.delete_many({})
    col.insert_many(emojii_emoticon_documents)


def load_stopwords():
    stopwords_documents = []
    for stop_word in stop_words:
        stopwords_documents.append({"Word": stop_word})

    db = client_master['TwitterEmotions']
    col = db.StopWord
    col.delete_many({})
    col.insert_many(stopwords_documents)


def load_slang():
    slang_documents = []
    for key in slang:
        slang_documents.append({"Slang": key, "Traduction": slang[key]})

    db = client_master['TwitterEmotions']
    col = db.Slang
    col.delete_many({})
    col.insert_many(slang_documents)


def load_negative_word():
    negative_word = []
    with open(path_negative_word, 'r', encoding="utf8") as reader:
        line = reader.readline()
        while line != '':
            negative_word.append({"Word": line.strip()})
            line = reader.readline()

    db = client_master['TwitterEmotions']
    col = db.NegativeWord
    col.delete_many({})
    col.insert_many(negative_word)


def load_tweet():
    tweets = []
    for filename in os.listdir(path_tweet):

        emotion = filename.split('_')[2]

        with open(path_tweet + '\\' + filename, 'r', encoding="utf8") as reader:
            line = reader.readline()
            while line != '':
                tweets.append({"Text": line, "Emotion": emotion, "Processed": 0})
                line = reader.readline()

    db = client_master['TwitterEmotions']
    col = db.Tweet
    col.delete_many({})

    col.insert_many(tweets)


def load_emotions():
    db = client_master['TwitterEmotions']
    col = db.WordCount
    col.delete_many({})
    for filename in os.listdir(path_emotions):
        col.insert_one({"Emotion": filename.lower()})
        word_dictionary = {}
        for filename2 in os.listdir(path_emotions + '\\' + filename):
            with open(path_emotions + '\\' + filename + '\\' + filename2, 'r', encoding="utf8") as reader:
                pos = 0
                if filename2[0:3] == "Emo":
                    pos = 0
                elif filename2[0:3] == "NRC":
                    pos = 1
                else:
                    pos = 2

                line = reader.readline()
                while line != '':
                    if line.strip().lower() in word_dictionary:
                        flag_array = word_dictionary[line.strip().lower()]
                        flag_array[pos] = 1
                        word_dictionary[line.strip().lower()] = flag_array
                    else:
                        flag_array = [0, 0, 0]
                        flag_array[pos] = 1
                        word_dictionary[line.strip().lower()] = flag_array

                    line = reader.readline()
        for key in word_dictionary:
            col.update_one({"Emotion": filename.lower()}, {'$push': {
                'Words': {'Word': key, 'Count': 0, "FlagEmoSN": word_dictionary[key][0],
                          "FlagNRC": word_dictionary[key][1], "FlagSentisense": word_dictionary[key][2]}}})


def initialise_cluster(mongos_data):
    global client_master
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])

    load_negative_word()
    load_slang()
    load_stopwords()
    load_emojii_emoticon()
    load_tweet()
    load_emotions()
