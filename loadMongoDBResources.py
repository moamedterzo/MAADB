import pymongo
import os
from slang_emojii_emoticon_stopwords import slang, stop_words, pos_emoticons, neg_emoticons, EmojiPos, EmojiNeg, \
    OthersEmoji

# import nltk
# nltk.download()
# from nltk.corpus import stopwords
# sw = stopwords.words("english")

numberOfClusters= 2
client = pymongo.MongoClient('localhost', 27017)
path_tweet = 'Risorse\Twitter messaggi'
path_negative_word = "Risorse\elenco-parole-che-negano-parole-successive.txt"

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
    for i in range(1, numberOfClusters + 1):
        print('cluster_' + str(i))
        db = client['cluster_' + str(i)]
        col = db.Emoticon
        col.insert_many(emojii_emoticon_documents)

def load_stopwords():
    stopwords_documents = []
    for stop_word in stop_words:
        stopwords_documents.append({"Word": stop_word})
    for i in range(1, numberOfClusters + 1):
        print('cluster_' + str(i))
        db = client['cluster_' + str(i)]
        col = db.StopWord
        col.insert_many(stopwords_documents)

def load_slang():
    slang_documents = []
    for key in slang:
        slang_documents.append({"Slang": key, "Traduction": slang[key]})

    for i in range(1, numberOfClusters + 1):
        print('cluster_' + str(i))
        db = client['cluster_' + str(i)]
        col = db.Slang
        col.insert_many(slang_documents)

def load_negative_word():
    negative_word = []
    with open(path_negative_word, 'r', encoding="utf8") as reader:
        line = reader.readline()
        while line != '':
            negative_word.append({"Word": line.strip()})
            line = reader.readline()

    for i in range(1, numberOfClusters + 1):
        print('cluster_' + str(i))
        db = client['cluster_' + str(i)]
        col = db.NegativeWord
        col.insert_many(negative_word)

def load_tweet():
    tweets = []
    for filename in os.listdir(path_tweet):
        with open(path_tweet + '\\' + filename, 'r', encoding="utf8") as reader:
            line = reader.readline()
            while line != '':
                tweets.append({"Text": line})
                line = reader.readline()
    start = 0
    size_of_split = int(len(tweets) / numberOfClusters)
    max_size = size_of_split
    for i in range(1, numberOfClusters + 1):
        print('cluster_' + str(i))
        db = client['cluster_' + str(i)]
        col = db.Tweet
        col.insert_many(tweets[start:max_size])
        start = start + size_of_split
        max_size = max_size + size_of_split




#load_tweet()
#load_negative_word()
#load_slang()
#load_stopwords()
#load_emojii_emoticon()