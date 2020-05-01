import mariadb
import resource_initializer as ri



def load_emojii_emoticon(conn, cursor):
    cursor.execute('delete from Emoticon')
    conn.commit()

    emojii_emoticon_documents = ri.load_emojii_emoticon()

    for emojicon in emojii_emoticon_documents:
        cursor.execute(
            "insert into Emoticon(Code, Polarity) values ('" + emojicon['Code'].replace("'", "''").replace("\\", "\\\\") + "', " + str(
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
            "insert into Slang(Slang, Traduction) values ('" + key.replace("'", "''") + "', '" + ri.slang[key].replace("'","''") + "')")

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
    tweets = ri.load_tweet()

    print('Inserting tweets...')

    n_tweets = len(tweets)
    batch_size = 120000 # se questo valore è troppo grande si rischia di far andare in errore l'inserimento
    curr_number = 0

    while curr_number < n_tweets:

        end_number = curr_number + batch_size - 1

        if end_number > n_tweets:
            cursor.executemany(sql_insert, tweets[curr_number:])
        else:
            cursor.executemany(sql_insert, tweets[curr_number:end_number])

        curr_number += batch_size

    conn.commit()
    print('Tweets inserted!')


def load_emotions(conn, cursor):
    cursor.execute('delete from WordCount')
    conn.commit()
    sql_insert = "insert into WordCount(Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN) VALUES (?, ?, ?, ?, ?, ?)"
    emotions = ri.load_emotions()

    print('Inserting Emotions...')
    cursor.executemany(sql_insert, emotions)
    conn.commit()
    print('Emotion inserted!')


def initialise_database(mariadb_setting):

    # Instantiate Connection
    conn = mariadb.connect(
        user=mariadb_setting['Username'],
        password=mariadb_setting['Password'],
        host=mariadb_setting['HostName'],
        port=mariadb_setting['Port'],
        database='TwitterEmotions')

    cursor = conn.cursor()

    load_negative_word(conn, cursor)
    load_slang(conn, cursor)
    load_stopwords(conn, cursor)
    load_emojii_emoticon(conn, cursor)
    load_tweet(conn, cursor)
    load_emotions(conn, cursor)

    cursor.close()
    conn.close()



def run_twitter_analisys(setting_data):

    print("Fine preprocess")

    print("Fine map reduce")
