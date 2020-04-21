import os
from slang_emojii_emoticon_stopwords import slang, stop_words, pos_emoticons, neg_emoticons, EmojiPos, EmojiNeg, \
    OthersEmoji
import pyodbc

path_tweet = '..\..\Risorse\Twitter messaggi'
path_negative_word = "..\..\Risorse\elenco-parole-che-negano-parole-successive.txt"
path_emotions = "..\..\Risorse\Sentimenti"


def load_emojii_emoticon(conn, cursor):
    cursor.execute('delete from Emoticon')
    conn.commit()

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

    for emojicon in emojii_emoticon_documents:
        cursor.execute(
            "insert into Emoticon(Code, Polarity) values ('" + emojicon['Code'].replace("'", "''") + "', " + str(
                emojicon['Polarity']) + ")")

    conn.commit()


def load_stopwords(conn, cursor):
    cursor.execute('delete from StopWord')
    conn.commit()

    for stop_word in stop_words:
        cursor.execute("insert into StopWord(Word) values ('" + stop_word.replace("'", "''") + "')")

    conn.commit()


def load_slang(conn, cursor):
    cursor.execute('delete from Slang')
    conn.commit()

    for key in slang:
        cursor.execute(
            "insert into Slang(Slang, Traduction) values ('" + key.replace("'", "''") + "', '" + slang[key].replace("'",
                                                                                                                    "''") + "')")

    conn.commit()


def load_negative_word(conn, cursor):
    cursor.execute("delete from NegativeWord")
    conn.commit()

    with open(path_negative_word, 'r', encoding="utf8") as reader:
        line = reader.readline()

        while line != '':
            cursor.execute("insert into NegativeWord(Word) values ('" + line.strip().replace("'", "''") + "')")
            line = reader.readline()

    conn.commit()


def load_tweet(conn, cursor):
    cursor.execute('delete from Tweet')
    conn.commit()

    sql_insert = "insert into Tweet(Text, Emotion) VALUES (?, ?)"
    params = []

    for filename in os.listdir(path_tweet):

        emotion = filename.split('_')[2]
        with open(path_tweet + '\\' + filename, 'r', encoding="utf8") as reader:

            line = reader.readline()
            while line != '':
                params.append((line.replace("'", "''"), emotion))
                line = reader.readline()

    print('Inserting tweets...')
    cursor.executemany(sql_insert, params)
    conn.commit()
    print('Tweets inserted!')


def load_emotions(conn, cursor):
    cursor.execute('delete from WordCount')
    conn.commit()
    sql_insert = "insert into WordCount(Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN) VALUES (?, ?, ?, ?, ?, ?)"
    for filename in os.listdir(path_emotions):
        params = []
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
            params.append((filename.lower(), key, 0, word_dictionary[key][0], word_dictionary[key][1],
                          word_dictionary[key][2]))

        print('Inserting Emotion...')
        cursor.executemany(sql_insert, params)
        conn.commit()
        print('Emotion inserted!')


def initialise_database():
    # todo prendere info da settings
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=localhost\SQLEXPRESS;"
                          "Database=TwitterEmotions;"
                          "Trusted_Connection=yes;")
    cursor = conn.cursor()

    try:

        # todo gestire risorse
        load_negative_word(conn, cursor)
        load_slang(conn, cursor)
        load_stopwords(conn, cursor)
        load_emojii_emoticon(conn, cursor)
        load_tweet(conn, cursor)
        load_emotions(conn, cursor)
        cursor.close()
        conn.close()
    except:
        cursor.close()
        conn.close()
        raise
