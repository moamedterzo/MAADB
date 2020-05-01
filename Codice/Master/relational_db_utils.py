import pyodbc

import resource_initializer as ri



def load_emojii_emoticon(conn, cursor):
    cursor.execute('delete from Emoticon')
    conn.commit()

    emojii_emoticon_documents = ri.load_emojii_emoticon()

    for emojicon in emojii_emoticon_documents:
        cursor.execute(
            "insert into Emoticon(Code, Polarity) values ('" + emojicon['Code'].replace("'", "''") + "', " + str(
                emojicon['Polarity']) + ")")

    conn.commit()


def load_stopwords(conn, cursor):
    cursor.execute('delete from StopWord')
    conn.commit()

    for stop_word in ri.stop_words:
        cursor.execute("insert into StopWord(Word) values ('" + stop_word.replace("'", "''") + "')")

    conn.commit()


def load_slang(conn, cursor):
    cursor.execute('delete from Slang')
    conn.commit()

    for key in ri.slang:
        cursor.execute(
            "insert into Slang(Slang, Traduction) values ('" + key.replace("'", "''") + "', '" + ri.slang[key].replace("'","''"))

    conn.commit()


def load_negative_word(conn, cursor):
    cursor.execute("delete from NegativeWord")
    conn.commit()

    neg_words = ri.load_negative_word()

    for line in neg_words:
        cursor.execute("insert into NegativeWord(Word) values ('" + line.strip().replace("'", "''") + "')")

    conn.commit()


def load_tweet(conn, cursor):
    cursor.execute('delete from Tweet')
    conn.commit()

    sql_insert = "insert into Tweet(Text, Emotion) VALUES (?, ?)"

    print('Inserting tweets...')
    cursor.executemany(sql_insert, ri.load_tweet())
    conn.commit()
    print('Tweets inserted!')


def load_emotions(conn, cursor):
    cursor.execute('delete from WordCount')
    conn.commit()
    sql_insert = "insert into WordCount(Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN) VALUES (?, ?, ?, ?, ?, ?)"

    print('Inserting Emotions...')
    cursor.executemany(sql_insert, ri.load_emotions())
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
