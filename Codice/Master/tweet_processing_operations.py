import json
import re
import pymongo
from Codice.Master.slang_emojii_emoticon_stopwords import punctuations
from nltk.stem import WordNetLemmatizer
from bson.code import Code

client_master = None
db = None

'''
CODICE SOLUZIONE PRECEDENTE

hashtag_map = {"anger": defaultdict(int), "anticipation": defaultdict(int), "disgust": defaultdict(int),
               "fear": defaultdict(int), "joy": defaultdict(int), "sadness": defaultdict(int),
               "surprise": defaultdict(int),
               "trust": defaultdict(int)}
emoji_map = {"anger": defaultdict(int), "anticipation": defaultdict(int), "disgust": defaultdict(int),
             "fear": defaultdict(int), "joy": defaultdict(int), "sadness": defaultdict(int),
             "surprise": defaultdict(int),
             "trust": defaultdict(int)}
emotion_words_map = {"anger": defaultdict(int), "anticipation": defaultdict(int), "disgust": defaultdict(int),
                     "fear": defaultdict(int), "joy": defaultdict(int), "sadness": defaultdict(int),
                     "surprise": defaultdict(int),
                     "trust": defaultdict(int)}
'''


def remove_nick_and_url(line):
    if "USERNAME" in line:
        line = line.replace("USERNAME", " ")
    if "URL" in line:
        line = line.replace("URL", " ")
    return line


def remove_punctuation(line):
    for punctuation in punctuations:
        if punctuation in line:
            line = line.replace(punctuation, "")
    return line


def remove_digits(line):
    return ''.join(" " if c.isdigit() else c for c in line)


def substitute_slangs(line, slang_dict):
    for slang in slang_dict:
        slang = " " + slang + " "
        standard = " " + slang_dict[slang.replace(" ", "")] + " "

        if line.startswith(slang[1::]):
            line = line.replace(slang[1::], standard) + line[len(slang[1::])::]
        if slang in line:
            line = line.replace(slang, standard)
        if line.endswith(slang[::-1]):
            line = line[:-len(slang[1::])] + line.replace(slang[::-1], standard)
    return line


def delete_emoji_and_emoticon(line, emoticon_list):
    for emoticon in emoticon_list:
        if emoticon in line:
            line = line.replace(emoticon, ' ')
    return line


def delete_stopwords(line, stop_word_list):
    for stop_word in stop_word_list:
        stop_word = " " + stop_word + " "

        if line.startswith(stop_word[1::]):
            line = line[len(stop_word[1::])::]
        if stop_word in line:
            line = line.replace(stop_word, " ")
        if line.endswith(stop_word[1:-1]):
            line = line[:-len(stop_word[::])]
    return line


def clean_tweet(line, emotion, emoticon_list, slang_dict, stop_word_list, emotion_words):
    '''
    CODICE SOLUZIONE PRECEDENTE

    for words in line.split():
        if words.startswith('#') and words != '#':
            hashtag_map[emotion][words] += 1
        if words in emoticon_list:
            emoji_map[emotion][words] += 1
    '''
    # Conteggio degli hashtag e delle emoticon
    line_words_emotion_list = []
    line_hashtag_list = []
    line_emoticon_list = []

    for words in line.split():
        if words.startswith('#') and words != '#':
            line_hashtag_list.append(words)
        if words in emoticon_list:
            line_emoticon_list.append(words)

    line = remove_nick_and_url(line)
    line = delete_emoji_and_emoticon(line, emoticon_list)
    line = remove_punctuation(line)
    line = remove_digits(line)
    line = line.lower()
    line = substitute_slangs(line, slang_dict)
    line = WordNetLemmatizer().lemmatize(line)
    line = delete_stopwords(line, stop_word_list)
    line = re.sub(' +', ' ', line.strip())
    '''
    CODICE SOLUZIONE PRECEDENTE
    for words in line.split():
        if words in emotion_words[emotion]:
            emotion_words_map[emotion][words] += 1
    '''
    # Count delle parole
    for words in line.split():
        if words in emotion_words[emotion]:
            line_words_emotion_list.append(words)

    return line, line_words_emotion_list, line_emoticon_list, line_hashtag_list


def findEmoticon():
    col = db.Emoticon
    emoticon_dict = {}
    for emoticon in col.find():
        if emoticon["Code"] not in emoticon_dict:
            emoticon_dict[emoticon["Code"]] = emoticon["Polarity"]
    return emoticon_dict


def findNegativeWord():
    col = db.NegativeWord
    negative_word_list = []
    for negative_word in col.find():
        negative_word_list.append(negative_word["Word"])
    return negative_word_list


def findSlang():
    col = db.Slang
    slang_dict = {}
    for slang in col.find():
        if slang["Slang"] not in slang_dict:
            slang_dict[slang["Slang"]] = slang["Traduction"]
    return slang_dict


def findTweet():
    col = db.Tweet
    tweet_list = []
    for tweet in col.find({"Processed": 0}):
        tweet_list.append([tweet["Text"], tweet["Emotion"], tweet["_id"]])
    return tweet_list


def findProcessedTweet():
    col = db.Tweet
    tweet_list = []
    for tweet in col.find({"Processed": 1}):
        tweet_list.append([tweet["Text"], tweet["Emotion"]])
    return tweet_list


def findStopWord():
    col = db.StopWord
    stop_word_list = []
    for stop_word in col.find():
        stop_word_list.append(stop_word["Word"])
    return stop_word_list


def findEmotionWords():
    col = db.WordCount
    emotion_words = {}
    for emotion in col.find():
        word_list = []
        for word in emotion["Words"]:
            word_list.append(word["Word"])
        emotion_words[emotion["Emotion"]] = word_list
    return emotion_words


def main():
    with open('setting.json') as json_file:
        setting_data = json.load(json_file)
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    global client_master
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    global db
    db = client_master['TwitterEmotions']
    col = db.Tweet
    emotion_words = findEmotionWords()
    tweet_list = findTweet()
    slang_dict = findSlang()
    emoticon_list = findEmoticon()
    stop_word_list = findStopWord()
    count = 0
    # tweets_processed = []
    # return line, line_words_emotion_list, line_emoticon_list, line_emoticon_list

    for tweet in tweet_list:
        processed_tweet, line_words_emotion_list, line_emoticon_list, line_hashtag_list = clean_tweet(tweet[0],
                                                                                                      tweet[1],
                                                                                                      emoticon_list,
                                                                                                      slang_dict,
                                                                                                      stop_word_list,
                                                                                                      emotion_words)
        col.update_one({"_id": tweet[2]}, {'$set': {
            'ProcessedTweet': processed_tweet, "Words": line_words_emotion_list, "Emoticon": line_emoticon_list,
            "Hashtag": line_hashtag_list}})

        # DELLA SOLUZIONE PRECEDENTE tweets_processed.append({"Text": processed_tweet, "Emotion": tweet[1], "Processed": 1})
        count = count + 1
        if (count % 10000) == 0:
            print("Processati " + str(count) + "/" + str(len(tweet_list)))


'''
CODICE SOLUZIONE PRECECEDENTE

def main():
    with open('setting.json') as json_file:
        setting_data = json.load(json_file)
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    global client_master
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    global db
    db = client_master['TwitterEmotions']
    emotion_words = findEmotionWords()
    tweet_list = findTweet()
    slang_dict = findSlang()
    emoticon_list = findEmoticon()
    stop_word_list = findStopWord()
    count = 0
    tweets_processed = []
    for tweet in tweet_list:
        processed_tweet = clean_tweet(tweet[0], tweet[1], emoticon_list, slang_dict, stop_word_list, emotion_words)
        tweets_processed.append({"Text": processed_tweet, "Emotion": tweet[1], "Processed": 1})
        count = count + 1
        if (count % 10000) == 0:
            print("Processati " + str(count) + "/" + str(len(tweet_list)))
            break;
    for emotion in emotion_words_map:
        for word in emotion_words_map[emotion]:
            print(word + " " + str(emotion_words_map[emotion][word]))
    # Memorizzazione Tweet Processati
    # col = db.Tweet
    # col.delete_many({"Processed": 1})
    # col.insert_many(tweets_processed)
    # Memorizzazione Hashtag count
    col = db.HashtagCount
    col.delete_many({})
    hashtag_documents = []
    for emotion in hashtag_map:
        for term in hashtag_map[emotion]:
            hashtag_documents.append({"Emotion": emotion, "Hashtag": term, "Count": hashtag_map[emotion][term]})
    col.insert_many(hashtag_documents)
    # Memorizzazione Emoticon count
    col = db.EmoticonCount
    emotion_documents = []
    for emotion in emoji_map:
        for emoticon in emoji_map[emotion]:
            emotion_documents.append({"Emotion": emotion, "Emoticon": emoticon, 'Count': emoji_map[emotion][emoticon]})
    col.delete_many({})
    col.insert_many(emotion_documents)
'''


def map():
    with open('setting.json') as json_file:
        setting_data = json.load(json_file)
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    global client_master
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    global db
    db = client_master['TwitterEmotions']

    with open("map_words.js", 'r') as file:
        map = Code(file.read())

    with open("reduce_words.js", 'r') as file:
        reduce = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults")

    f = open("map_reduce_word.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc)+"\n")
    f.close()

    with open("map_emoticon.js", 'r') as file:
        map = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults")
    f = open("map_reduce_emoticon.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc)+"\n")
    f.close()

    with open("map_hashtag.js", 'r') as file:
        map = Code(file.read())

    result = db.Tweet.map_reduce(map, reduce, "myresults", )
    f = open("map_reduce_hashtag.txt", "a", encoding="utf-8")
    for doc in result.find():
        f.write(str(doc)+"\n")
    f.close()

#main()
map()
