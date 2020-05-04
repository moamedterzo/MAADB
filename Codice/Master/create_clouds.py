from collections import defaultdict
import pymongo
from wordcloud import WordCloud, STOPWORDS
import mariadb
import numpy as np
from PIL import Image


def print_cloud(frequency_list, path, width, height):
    twitter_mask = np.array(Image.open("twitter_logo.png"))

    stopwords = set(STOPWORDS)

    wordcloud = WordCloud(background_color="white", mask=twitter_mask, width=width, height=height, stopwords=stopwords)

    wordcloud.generate_from_frequencies(frequency_list)

    wordcloud.to_file("output_clouds/" + path + ".png")


def make_clouds(setting_data, dbms):
    if dbms == 1:
        mongos_data = setting_data
        client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
        db = client_master['TwitterEmotions']
        col = db.WordCount
        emotion_list = []
        for document in col.find({}):
            emotion_list.append({"Emotion": document["_id"], "Words": document["values"]})
        for emotion in emotion_list:
            words = {}
            total = 0
            for word in emotion["Words"]:
                if int(word['count']) >= 5:
                    words.update({word['word']: int(word['count'])})
                    total += int(word['count'])
            for key in words:
                words.update({key: int(words[key]) / total})

            print_cloud(words, "words_" + emotion["Emotion"], 1000, 1000)
            print("Generata word cloud delle parole per l'emozione " + emotion["Emotion"])

        col = db.HashtagCount
        emotion_list = []
        for document in col.find({}):
            emotion_list.append({"Emotion": document["_id"], "Hashtags": document["values"]})
        for emotion in emotion_list:
            hashtags = {}
            total = 0
            for hashtag in emotion["Hashtags"]:
                if int(hashtag['count']) >= 5:
                    hashtags.update({hashtag['hashtag']: int(hashtag['count'])})
                    total += int(hashtag['count'])
            for key in hashtags:
                hashtags.update({key: int(hashtags[key]) / total})

            print_cloud(hashtags, "hashtags_" + emotion["Emotion"], 1000, 1000)
            print("Generata word cloud degli hashtag per l'emozione " + emotion["Emotion"])

        col = db.EmoticonCount
        emotion_list = []
        for document in col.find({}):
            emotion_list.append({"Emotion": document["_id"], "Emoticons": document["values"]})
        for emotion in emotion_list:
            emoticons = {}
            total = 0
            for emoticon in emotion["Emoticons"]:
                if int(emoticon['count']) >= 5:
                    emoticons.update({emoticon['emoticon']: int(emoticon['count'])})
                    total += int(emoticon['count'])
            for key in emoticons:
                emoticons.update({key: int(emoticons[key]) / total})

            print_cloud(emoticons, "emoticons_" + emotion["Emotion"], 500, 500)
            print("Generata word cloud delle emoticon per l'emozione " + emotion["Emotion"])

    else:
        conn = mariadb.connect(
            user=setting_data['Username'],
            password=setting_data['Password'],
            host=setting_data['HostName'],
            port=setting_data['Port'],
            database=setting_data['DatabaseName'])

        cursor = conn.cursor()
        emotion_dict = defaultdict(dict)
        cursor.execute("SELECT Emotion, Word, Count FROM wordcount")
        for (Emotion, Word, Count) in cursor:
            emotion_dict[Emotion][Word] = Count
        for emotion in emotion_dict:
            words = {}
            total = 0
            for word in emotion_dict[emotion]:
                if int(emotion_dict[emotion][word]) >= 5:
                    words.update({word: int(emotion_dict[emotion][word])})
                    total += int(emotion_dict[emotion][word])
            for key in words:
                words.update({key: int(words[key]) / total})

            print_cloud(words, "words_" + emotion, 1000, 1000)
            print("Generata word cloud delle parole per l'emozione " + emotion)

        hashtag_dict = defaultdict(dict)
        cursor.execute("SELECT Emotion, Hashtag, Count FROM hashtagcount")
        for (Emotion, Hashtag, Count) in cursor:
            hashtag_dict[Emotion][Hashtag] = Count
        for emotion in hashtag_dict:
            hashtags = {}
            total = 0
            for hashtag in hashtag_dict[emotion]:
                if int(hashtag_dict[emotion][hashtag]) >= 5:
                    hashtags.update({hashtag: int(hashtag_dict[emotion][hashtag])})
                    total += int(hashtag_dict[emotion][hashtag])
            for key in hashtags:
                hashtags.update({key: int(hashtags[key]) / total})

            print_cloud(hashtags, "hashtags_" + emotion, 1000, 1000)
            print("Generata word cloud degli hashtag per l'emozione " + emotion)

        emoticon_dict = defaultdict(dict)
        cursor.execute(
            "SELECT emoticoncount.Emotion, emoticoncount.COUNT, emoticon.Code FROM emoticoncount INNER JOIN emoticon ON emoticoncount.IDEmoticon=emoticon.ID")
        for (Emotion, Count, Code) in cursor:
            emoticon_dict[Emotion][Code] = Count
        for emotion in emoticon_dict:
            emoticons = {}
            total = 0
            for emoticon in emoticon_dict[emotion]:
                if int(emoticon_dict[emotion][emoticon]) >= 5:
                    emoticons.update({emoticon: int(emoticon_dict[emotion][emoticon])})
                    total += int(emoticon_dict[emotion][emoticon])
            for key in emoticons:
                emoticons.update({key: int(emoticons[key]) / total})

            print_cloud(emoticons, "emoticons_" + emotion, 500, 500)
            print("Generata word cloud delle emoticon per l'emozione " + emotion)


def stats(setting_data, dbms):
    if dbms == 1:
        print("Stiamo lavorando per voi")
    else:
        conn = mariadb.connect(
            user=setting_data['Username'],
            password=setting_data['Password'],
            host=setting_data['HostName'],
            port=setting_data['Port'],
            database=setting_data['DatabaseName'])
        cursor = conn.cursor()
        cursor.execute("SELECT Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN FROM wordcount")

        wordcount_documents = ddict()

        for (Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN) in cursor:
            wordcount_documents[Emotion][Word] = {"Count": Count, "FlagSentisense": FlagSentisense, "FlagNRC": FlagNRC,
                                                  "FlagEmoSN": FlagEmoSN}

        for emotion in wordcount_documents:
            total_type_useful = {"FlagSentisense": 0, "FlagNRC": 0, "FlagEmoSN": 0}
            total_type_unuseful = {"FlagSentisense": 0, "FlagNRC": 0, "FlagEmoSN": 0}
            new_word = []
            unuseful_words = []
            threshold_to_consider_a_word_frequent = 20

            for word in wordcount_documents[emotion]:
                flag_Sentisense = wordcount_documents[emotion][word]["FlagSentisense"]
                flag_NRC = wordcount_documents[emotion][word]["FlagNRC"]
                flag_EmoSN = wordcount_documents[emotion][word]["FlagEmoSN"]
                if wordcount_documents[emotion][word]["Count"] > 0:
                    if flag_Sentisense == 0 and flag_NRC == 0 and flag_EmoSN == 0:
                        new_word.append(
                            [wordcount_documents[emotion][word], wordcount_documents[emotion][word]["Count"]])
                    else:
                        total_type_useful["FlagSentisense"] += flag_Sentisense
                        total_type_useful["FlagNRC"] += flag_NRC
                        total_type_useful["FlagEmoSN"] += flag_EmoSN
                else:
                    unuseful_words.append(word)
                    total_type_unuseful["FlagSentisense"] += flag_Sentisense
                    total_type_unuseful["FlagNRC"] += flag_NRC
                    total_type_unuseful["FlagEmoSN"] += flag_EmoSN

            print("Emozione " + emotion + "\n")
            if total_type_useful["FlagSentisense"] + total_type_unuseful["FlagSentisense"] != 0:
                print("Sentisense parole che hanno contribuito all'analisi dei tweet: ",
                      total_type_useful["FlagSentisense"],
                      "/", (total_type_useful["FlagSentisense"] + total_type_unuseful["FlagSentisense"]), "\n")
            if total_type_useful["FlagNRC"] + total_type_unuseful["FlagNRC"]:
                print("NRC parole che hanno contribuito all'analisi dei tweet: ", total_type_useful["FlagNRC"], "/",
                      (total_type_useful["FlagNRC"] + total_type_unuseful["FlagNRC"]), "\n")
            if total_type_useful["FlagEmoSN"] + total_type_unuseful["FlagEmoSN"]:
                print("EmoSN parole che hanno contribuito all'analisi dei tweet: ", total_type_useful["FlagEmoSN"], "/",
                      (total_type_useful["FlagEmoSN"] + total_type_unuseful["FlagEmoSN"]), "\n")
            print("Son state inoltre trovate ", len(new_word), " parole che potrebbero arricchire il dizionario \n")
            frequent = 0
            for word in new_word:
                if word[1] > threshold_to_consider_a_word_frequent:
                    frequent += 1
            print("Di queste", frequent, "sono particolarmente frequenti poichè occorrono più di", threshold_to_consider_a_word_frequent, "volte\n")

def ddict():
    return defaultdict(ddict)
