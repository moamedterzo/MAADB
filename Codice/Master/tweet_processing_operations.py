import pymongo
from nltk.stem import PorterStemmer
from Codice.Master.slang_emojii_emoticon_stopwords import punctuations
from nltk.stem import WordNetLemmatizer

client_slaves = [
    pymongo.MongoClient('localhost', 27019),
    pymongo.MongoClient('localhost', 27020),
]


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


def clean_tweet(line, emoticon_list, slang_dict, stop_word_list):
    line = remove_nick_and_url(line)
    line = delete_emoji_and_emoticon(line,emoticon_list)
    line = remove_punctuation(line)
    line = remove_digits(line)
    line = line.lower()
    line = substitute_slangs(line, slang_dict)
    # line = nltk.word_tokenize(line)  # toglie anche la punteggiatura
    # line = nltk.pos_tag(line)  # ha senso farlo se poi non uso i tag?
    # line = do_stemming(line)
    # WordNetLemmatizer().lemmatize(line) può prendere in considerazione il tag se si fa pos tag
    line = WordNetLemmatizer().lemmatize(line)
    line = delete_stopwords(line, stop_word_list)
    return line


def findEmoticon(client):
    db = client['TwitterEmotionsSlave']
    col = db.Emoticon
    emoticon_dict = {}
    for emoticon in col.find():
        if emoticon["Code"] not in emoticon_dict:
            emoticon_dict[emoticon["Code"]] = emoticon["Polarity"]
    return emoticon_dict


def findNegativeWord(client):
    db = client['TwitterEmotionsSlave']
    col = db.NegativeWord
    negative_word_list = []
    for negative_word in col.find():
        negative_word_list.append(negative_word["Word"])
    return negative_word_list


def findSlang(client):
    db = client['TwitterEmotionsSlave']
    col = db.Slang
    slang_dict = {}
    for slang in col.find():
        if slang["Slang"] not in slang_dict:
            slang_dict[slang["Slang"]] = slang["Traduction"]
    return slang_dict


def findTweet(client):
    db = client['TwitterEmotionsSlave']
    col = db.Tweet
    tweet_list = []
    for tweet in col.find():
        tweet_list.append([tweet["Text"], tweet["Emotion"]])
    return tweet_list


def findStopWord(client):
    db = client['TwitterEmotionsSlave']
    col = db.StopWord
    stop_word_list = []
    for stop_word in col.find():
        stop_word_list.append(stop_word["Word"])
    return stop_word_list

def prova():
    tweet_list = findTweet(client_slaves[0])
    slang_dict = findSlang(client_slaves[0])
    emoticon_list = findEmoticon(client_slaves[0])
    stop_word_list = findStopWord(client_slaves[0])
    for tweet in tweet_list:
        #print(tweet[0])
        print(clean_tweet(tweet[0], emoticon_list, slang_dict, stop_word_list))
# print(findEmoticon(client_slaves[0]))
# print(findNegativeWord(client_slaves[0]))
# print(findSlang(client_slaves[0]))
# print(findTweet(client_slaves[0]))
# print(findStopWord(client_slaves[0]))

prova()