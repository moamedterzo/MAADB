import pymongo
import os

numberOfClusters = 2
client = pymongo.MongoClient('localhost', 27017)
path_tweet = 'Risorse\Twitter messaggi'
path_negative_word = "Risorse\elenco-parole-che-negano-parole-successive.txt"

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
load_negative_word()
