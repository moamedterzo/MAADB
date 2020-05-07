import mariadb
import resource_manager as ri
import tweet_processing as tp
import cloud_utils as cu


def load_emojicon(conn, cursor):
    cursor.execute('delete from EmoticonCount')
    cursor.execute('delete from Emoticon')
    conn.commit()

    for emojicon in ri.get_emojicon():
        cursor.execute(
            "insert into Emoticon(Code, Polarity) values ('" + emojicon['Code'].replace("'", "''").replace("\\", "\\\\") + "', " +
            str(emojicon['Polarity']) + ")")

    conn.commit()


def load_stopwords(conn, cursor):
    cursor.execute('delete from StopWord')
    conn.commit()

    for stop_word in ri.get_stop_words():
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

    neg_words = ri.get_negative_word()

    for line in neg_words:
        cursor.execute("insert into NegativeWord(Word) values ('" + line.strip().replace("'", "''") + "')")

    conn.commit()


def load_tweet(conn, cursor):
    cursor.execute('delete from Tweet')
    conn.commit()

    sql_insert = "insert into Tweet(Text, Emotion) VALUES (?, ?)"
    tweets = ri.get_tweets()

    # se questo valore è troppo grande si rischia di far andare in errore l'inserimento dei dati
    batch_size = 120000
    curr_number = 0
    n_tweets = len(tweets)

    while curr_number < n_tweets:

        end_number = curr_number + batch_size - 1

        if end_number > n_tweets:
            cursor.executemany(sql_insert, tweets[curr_number:])
        else:
            cursor.executemany(sql_insert, tweets[curr_number:end_number])

        curr_number += batch_size

    conn.commit()


def load_word_resources(conn, cursor):
    cursor.execute('delete from WordCount')
    conn.commit()

    sql_insert = "insert into WordCount(Emotion, Word, Count, FlagSentisense, FlagNRC, FlagEmoSN) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.executemany(sql_insert, ri.get_resources())
    conn.commit()


def initialise_database(mariadb_setting):

    # Si istanzia la connessione
    conn = mariadb.connect(
        user=mariadb_setting['Username'],
        password=mariadb_setting['Password'],
        host=mariadb_setting['HostName'],
        port=mariadb_setting['Port'],
        database=mariadb_setting['DatabaseName'])

    # si ottiene il cursore dei dati
    cursor = conn.cursor()

    print('Inserting negative words')
    load_negative_word(conn, cursor)
    print('Inserting slang')
    load_slang(conn, cursor)
    print('Inserting stop words')
    load_stopwords(conn, cursor)
    print('Inserting emoji and emoticons')
    load_emojicon(conn, cursor)
    print('Inserting tweets')
    load_tweet(conn, cursor)
    print('Inserting word resources')
    load_word_resources(conn, cursor)

    cursor.close()
    conn.close()

    print("Initialization completed!")


def run_twitter_analysis(mariadb_setting):

    # si ottiene la connessione
    conn = mariadb.connect(
        user=mariadb_setting['Username'],
        password=mariadb_setting['Password'],
        host=mariadb_setting['HostName'],
        port=mariadb_setting['Port'],
        database=mariadb_setting['DatabaseName'])

    print("Processing all tweets")
    words, hashtags, emoticons = preprocess_all_tweets(conn)

    print("Storing results")
    store_results(conn, words, hashtags, emoticons)

    print("Analysis completed")


def preprocess_all_tweets(conn):

    cursor = conn.cursor()

    # ottenimento dati di supporto
    slang_dict = find_slang(cursor)
    emoticon_list = find_emojicon(cursor)
    stop_word_list = find_stop_words(cursor)
    neg_word_list = find_negative_words(cursor)

    count = 0

    result_word_count = {}
    result_emojicon_count = {}
    result_hashtag_count = {}

    tweet_list = find_tweets(cursor)
    for tweet in tweet_list:
        # processamento tweet
        words, emoticons, hashtags = tp.process_tweet(tweet[0], emoticon_list, slang_dict, stop_word_list, neg_word_list)

        emotion = tweet[1]

        # aggiornamento del conteggio per parole, emoji, emoticons e hashtag
        if len(words) > 0:
            if emotion not in result_word_count:
                result_word_count[emotion] = {}

            for word in words:
                if word not in result_word_count[emotion]:
                    result_word_count[emotion][word] = 1
                else:
                    result_word_count[emotion][word] += 1

        if len(emoticons) > 0:
            if emotion not in result_emojicon_count:
                result_emojicon_count[emotion] = {}

            for emoticon in emoticons:
                id_emoticon = get_id_emojicon_from_code(emoticon, cursor)
                if id_emoticon not in result_emojicon_count[emotion]:
                    result_emojicon_count[emotion][id_emoticon] = 1
                else:
                    result_emojicon_count[emotion][id_emoticon] += 1

        if len(hashtags) > 0:
            if emotion not in result_hashtag_count:
                result_hashtag_count[emotion] = {}

            for hashtag in hashtags:
                if hashtag not in result_hashtag_count[emotion]:
                    result_hashtag_count[emotion][hashtag] = 1
                else:
                    result_hashtag_count[emotion][hashtag] += 1

        count += 1
        if count % 40000 == 0:
            print("Processed " + str(count) + "/" + str(len(tweet_list)))

    return result_word_count, result_hashtag_count,result_emojicon_count


def find_stop_words(cur):
    stop_word_list = []

    cur.execute("SELECT word FROM stopword")

    for (stop_word,) in cur:
        stop_word_list.append(stop_word)
    return stop_word_list


def find_negative_words(cur):
    neg_word_list = []

    cur.execute("SELECT word FROM negativeword")

    for (word,) in cur:
        neg_word_list.append(word)
    return neg_word_list


def find_emojicon(cur):

    emoticon_dict = {}
    cur.execute("SELECT code, polarity FROM emoticon")

    for (code, polarity) in cur:
        if code not in emoticon_dict:
            emoticon_dict[code] = polarity
    return emoticon_dict


def find_slang(cur):

    slang_dict = {}
    cur.execute("SELECT slang, traduction FROM slang")

    for (slang, traduction) in cur:
        if slang not in slang_dict:
            slang_dict[slang] = traduction

    return slang_dict


def find_tweets(cur):

    tweet_list = []

    cur.execute("SELECT text, emotion FROM tweet")

    for (text, emotion) in cur:
        tweet_list.append([text, emotion])

    return tweet_list


def store_results(conn, words_count, hashtags_count, emojicons_count):

    cursor = conn.cursor()

    # memorizzazione conteggi emoticons and emoji
    cursor.execute("delete from emoticoncount")

    data_emojicons = []
    for emotion in emojicons_count:
        emojicon_count_by_emo = emojicons_count[emotion]

        for id_emojicon in emojicon_count_by_emo:
            data_emojicons.append((emotion, id_emojicon, emojicon_count_by_emo[id_emojicon]))

    cursor.executemany("insert into emoticoncount(Emotion, IDEmoticon, Count) values (?,?,?)", data_emojicons)
    del data_emojicons

    # memorizzazione conteggi hashtag
    cursor.execute("delete from hashtagcount")

    data_hashtag = []
    for emotion in hashtags_count:
        hashtag_count_by_emo = hashtags_count[emotion]

        for hashtag in hashtag_count_by_emo:
            count = hashtag_count_by_emo[hashtag]
            data_hashtag.append((emotion, hashtag, count, count))

    cursor.executemany("insert into hashtagcount(Emotion, Hashtag, Count) values (?,?,?) on duplicate key update count = count + ?", data_hashtag)
    del data_hashtag

    # memorizzazione conteggi parole
    cursor.execute("delete from wordcount where flagnrc = 0 and flagsentisense = 0 and flagemosn = 0") #rimuovo tutte le parole non presenti nelle risorse
    data_word = []
    for emotion in words_count:
        word_count_by_emo = words_count[emotion]

        for word in word_count_by_emo:
            count = word_count_by_emo[word]
            data_word.append((count, emotion, word, count))

    cursor.executemany("insert into wordcount (count, emotion, word) values (?, ?, ?) on duplicate key update count = ?", data_word)
    del data_word


def get_id_emojicon_from_code(code, cursor):
    cursor.execute("SELECT ID FROM emoticon where code='" + code.replace('\'','\'\'').replace('\\','\\\\') +"' limit 1")
    for (ID,) in cursor:
        return ID

    # se non esiste l'elemento, lo creo
    cursor.executemany("INSERT INTO emoticon(code, polarity) values (?, ?)", [(code, 0)])

    cursor.execute("SELECT ID FROM emoticon where code='" + code.replace('\'','\'\'').replace('\\','\\\\') +"' limit 1")
    for (ID,) in cursor:
        return ID

#todo sistemare
def get_resources_stats(setting_data):
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
        print("Di queste", frequent, "sono particolarmente frequenti poichè occorrono più di",
              threshold_to_consider_a_word_frequent, "volte\n")

def create_clouds(setting_data):
    conn = mariadb.connect(
        user=setting_data['Username'],
        password=setting_data['Password'],
        host=setting_data['HostName'],
        port=setting_data['Port'],
        database=setting_data['DatabaseName'])

    cursor = conn.cursor()

    # ottenimento conteggi parole, hashtag ed emoticons

    #raggruppamento dei conteggi per parola
    cursor.execute("SELECT Emotion, Word, Count FROM wordcount where count > 0")
    word_count = {}
    for (emotion, word, count) in cursor:
        if word not in word_count:
            word_count[word] = {}
        word_count[word][emotion] = count

    cursor.execute("SELECT Emotion, Hashtag, Count FROM hashtagcount")
    hashtag_count = []
    for (emotion, hashtag, count) in cursor:
        hashtag_count.append({"Hashtag": hashtag, "Emotion": emotion, "Count": count})

    cursor.execute(
        "SELECT emoticoncount.Emotion, emoticoncount.COUNT, emoticon.Code FROM emoticoncount INNER JOIN emoticon ON emoticoncount.IDEmoticon=emoticon.ID")
    emojicon_count = []
    for (emotion, count, code) in cursor:
        emojicon_count.append({"Code": code, "Emotion": emotion, "Count": count})

    cu.make_clouds(word_count, hashtag_count, emojicon_count)




