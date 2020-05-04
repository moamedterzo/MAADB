import mariadb
import resource_initializer as ri
import tweet_processing as tp


def load_emojii_emoticon(conn, cursor):
    cursor.execute('delete from EmoticonCount')
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
        database=mariadb_setting['DatabaseName'])

    cursor = conn.cursor()

    print('Inserting neg words')
    load_negative_word(conn, cursor)
    print('Inserting slang')
    load_slang(conn, cursor)
    print('Inserting stop words')
    load_stopwords(conn, cursor)
    print('Inserting emoji and emoticons')
    load_emojii_emoticon(conn, cursor)
    print('Inserting tweets')
    load_tweet(conn, cursor)
    print('Inserting word resources')
    load_emotions(conn, cursor)

    cursor.close()
    conn.close()



def run_twitter_analisys(mariadb_setting):

    conn = mariadb.connect(
        user=mariadb_setting['Username'],
        password=mariadb_setting['Password'],
        host=mariadb_setting['HostName'],
        port=mariadb_setting['Port'],
        database=mariadb_setting['DatabaseName'])

    (words, hashtags, emoticons) = preprocess_all_tweets(conn)
    print("Fine preprocess + conteggio")

    store_results(conn, words, hashtags, emoticons)
    print("Fine memorizzazione risultati")


def preprocess_all_tweets(conn):

    cursor = conn.cursor()

    slang_dict = findSlang(cursor)
    emoticon_list = findEmoticon(cursor)
    stop_word_list = findStopWord(cursor)

    tweet_list = findTweet(cursor)

    count = 0


    result_word_count = {}
    result_emoticon_count = {}
    result_hashtag_count = {}

    for tweet in tweet_list:
        words, emoticons, hashtags = tp.process_tweet(tweet[0], emoticon_list, slang_dict, stop_word_list)

        emotion = tweet[1]

        if len(words) > 0:
            if emotion not in result_word_count:
                result_word_count[emotion] = {}

            for word in words:
                if word not in result_word_count[emotion]:
                    result_word_count[emotion][word] = 1
                else:
                    result_word_count[emotion][word] += 1

        if len(emoticons) > 0:
            if emotion not in result_emoticon_count:
                result_emoticon_count[emotion] = {}

            for emoticon in emoticons:
                id_emoticon = get_id_emoticon_from_code(emoticon, cursor)
                if id_emoticon not in result_emoticon_count[emotion]:
                    result_emoticon_count[emotion][id_emoticon] = 1
                else:
                    result_emoticon_count[emotion][id_emoticon] += 1

        if len(hashtags) > 0:
            if emotion not in result_hashtag_count:
                result_hashtag_count[emotion] = {}

            for hashtag in hashtags:
                if hashtag not in result_hashtag_count[emotion]:
                    result_hashtag_count[emotion][hashtag] = 1
                else:
                    result_hashtag_count[emotion][hashtag] += 1

        count = count + 1
        if (count % 10000) == 0:
            print("Processati " + str(count) + "/" + str(len(tweet_list)))



    return result_word_count, result_hashtag_count,result_emoticon_count


def findStopWord(cur):
    stop_word_list = []

    cur.execute("SELECT word FROM stopword")

    for (stop_word,) in cur:
        stop_word_list.append(stop_word)
    return stop_word_list


def findEmoticon(cur):

    emoticon_dict = {}
    cur.execute("SELECT code, polarity FROM emoticon")

    for (code, polarity) in cur:
        if code not in emoticon_dict:
            emoticon_dict[code] = polarity
    return emoticon_dict


def findSlang(cur):

    slang_dict = {}
    cur.execute("SELECT slang, traduction FROM slang")

    for (slang, traduction) in cur:
        if slang not in slang_dict:
            slang_dict[slang] = traduction

    return slang_dict


def findTweet(cur):

    tweet_list = []

    cur.execute("SELECT text, emotion FROM tweet")

    for (text, emotion) in cur:
        tweet_list.append([text, emotion])

    return tweet_list


def store_results(conn, words_count, hashtags_count, emoticons_count):

    cursor = conn.cursor()


    cursor.execute("delete from emoticoncount")
    data_emoticon = []
    for emotion in emoticons_count:
        emoticon_count_by_emo = emoticons_count[emotion]

        for id_emoticon in emoticon_count_by_emo:
            data_emoticon.append((emotion, id_emoticon, emoticon_count_by_emo[id_emoticon]))

    cursor.executemany("insert into emoticoncount(Emotion, IDEmoticon, Count) values (?,?,?)", data_emoticon)
    del data_emoticon


    cursor.execute("delete from hashtagcount")
    data_hashtag = []
    for emotion in hashtags_count:
        hashtag_count_by_emo = hashtags_count[emotion]

        for hashtag in hashtag_count_by_emo:
            count = hashtag_count_by_emo[hashtag]
            data_hashtag.append((emotion, hashtag, count, count))

    cursor.executemany("insert into hashtagcount(Emotion, Hashtag, Count) values (?,?,?) on duplicate key update count = count + ?", data_hashtag)
    del data_hashtag


    cursor.execute("delete from wordcount where flagnrc = 0 and flagsentisense = 0 and flagemosn = 0") #rimuovo tutte le parole non presenti nelle risorse
    data_word = []
    for emotion in words_count:
        word_count_by_emo = words_count[emotion]

        for word in word_count_by_emo:
            count = word_count_by_emo[word]
            data_word.append((count, emotion, word, count))

    cursor.executemany("insert into wordcount (count, emotion, word) values (?, ?, ?) on duplicate key update count = ?", data_word)

    del data_word




def get_id_emoticon_from_code(code, cursor):
    cursor.execute("SELECT ID FROM emoticon where code='" + code.replace('\'','\'\'').replace('\\','\\\\') +"' limit 1")
    for (ID,) in cursor:
        return ID

    #se non esiste, lo creo al volo
    cursor.executemany("INSERT INTO emoticon(code, polarity) values (?, ?)", [(code, 0)])

    cursor.execute("SELECT ID FROM emoticon where code='" + code.replace('\'','\'\'').replace('\\','\\\\') +"' limit 1")
    for (ID,) in cursor:
        return ID
