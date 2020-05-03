from collections import defaultdict
import pymongo
from wordcloud import WordCloud, STOPWORDS
import mariadb


def print_cloud(frequency_list, path, width, height):
    stopwords = set(STOPWORDS)
    wordcloud = WordCloud(width=width, height=height,
                          background_color='white',
                          stopwords=stopwords,
                          min_font_size=10).generate_from_frequencies(frequency_list)
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
