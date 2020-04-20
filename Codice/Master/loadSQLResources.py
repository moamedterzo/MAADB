import os
from slang_emojii_emoticon_stopwords import slang, stop_words, pos_emoticons, neg_emoticons, EmojiPos, EmojiNeg, \
    OthersEmoji
import pyodbc


path_tweet = '..\..\Risorse\Twitter messaggi'
path_negative_word = "..\..\Risorse\elenco-parole-che-negano-parole-successive.txt"



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

    cursor.execute("delete from Emoticon")
    for emojicon in emojii_emoticon_documents:
        cursor.execute("insert into Emoticon(Code, Polarity) values ('" + emojicon['Code'].replace("'", "''") + "', " + str(emojicon['Polarity']) + ")")

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
        cursor.execute("insert into Slang(Slang, Traduction) values ('" + key.replace("'", "''") + "', '" + slang[key].replace("'", "''") + "')")

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

    command = ''

    sql_insert = "insert into Tweet(Text, Emotion) VALUES (?, ?)"
    params = []



    for filename in os.listdir(path_tweet):

        emotion = filename.split('_')[2]
        with open(path_tweet + '\\' + filename, 'r', encoding="utf8") as reader:

            line = reader.readline()
            while line != '':

                params.append((line.replace("'", "''"), emotion))
                #command += "insert into Tweet(Text, Emotion) values ('" + line.replace("'",
                 #                                                                      "''") + "', '" + emotion + "')\n"
                line = reader.readline()

    print('Inserting tweets...')
    #cursor.execute(command)
    cursor.executemany(sql_insert, params)
    conn.commit()
    print('Tweets inserted!')


def initialise_database():
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
        #load_tweet(conn, cursor)

        cursor.close()
        conn.close()
    except:
        cursor.close()
        conn.close()
        raise
