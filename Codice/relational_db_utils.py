from collections import defaultdict
import mariadb
from matplotlib import pyplot as plt
import resource_manager as ri
import tweet_processing as tp
import cloud_utils as cu

threshold_frequent_word = 10


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

    sql_insert = "insert into WordCount(Emotion, Word, Count, FlagEmoSN, FlagNRC, FlagSentisense) VALUES (?, ?, ?, ?, ?, ?)"
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


def get_resources_stats(setting_data):
    conn = mariadb.connect(
        user=setting_data['Username'],
        password=setting_data['Password'],
        host=setting_data['HostName'],
        port=setting_data['Port'],
        database=setting_data['DatabaseName'])

    cursor = conn.cursor()

    word_counts = defaultdict()

    # ottengo numero totale di nuove parole conteggiate non presenti in ciascuna delle risorse
    cursor.execute(
        "SELECT Emotion, COUNT(*) FROM wordcount WHERE flagsentisense = 0 AND FlagNRC = 0 AND FlagEmoSN = 0 and COUNT > " + str(
            threshold_frequent_word) + " GROUP BY emotion")
    for (emotion, count) in cursor: word_counts[emotion] = [count]

    # totale delle parole conteggiate presenti anche nelle risorse
    cursor.execute("SELECT Emotion, sum(flagsentisense), SUM(flagNRC) , SUM(flagemosn)\
                    FROM wordcount\
                    WHERE COUNT > 0\
                    GROUP BY emotion")

    for (emotion, sentisense, nrc, emosn) in cursor:
        word_counts[emotion].extend([sentisense, nrc, emosn])

    # totale delle parole presenti nelle risorse
    cursor.execute("SELECT Emotion, sum(flagsentisense), SUM(flagNRC) , SUM(flagemosn)\
                    FROM wordcount\
                    GROUP BY emotion")

    for (emotion, sentisense, nrc, emosn) in cursor:
        word_counts[emotion].extend([sentisense, nrc, emosn])

    # totale delle parole conteggiate (il calcolo varia per ciascuna risorsa)
    # seleziono il totale delle parole conteggiate non presenti nella risorsa, e le sommo a quelle conteggiate presenti nella risorsa
    cursor.execute("SELECT Emotion, COUNT(*) FROM wordcount WHERE flagsentisense = 0 and COUNT >= "+ str(threshold_frequent_word) +" GROUP BY emotion")
    for (emotion, res_count) in cursor: word_counts[emotion].append(res_count + word_counts[emotion][0])

    cursor.execute("SELECT Emotion, COUNT(*) FROM wordcount WHERE FlagNRC = 0 and COUNT >= "+ str(threshold_frequent_word) +" GROUP BY emotion")
    for (emotion, res_count) in cursor: word_counts[emotion].append(res_count + word_counts[emotion][1])

    cursor.execute("SELECT Emotion, COUNT(*) FROM wordcount WHERE FlagEmoSN = 0 and COUNT >= "+ str(threshold_frequent_word) +" GROUP BY emotion")
    for (emotion, res_count) in cursor: word_counts[emotion].append(res_count + word_counts[emotion][2])

    # calcolo e stampa dei risultati4
    print("\nShowing results for threshold=" + str(threshold_frequent_word))
    print()

    print("EMOTION      |   % TWEETS -> RESOURCES        || % RESOURCES-> TWEETS          || NEW WORDS")
    print("             |     Sentisense |  NRC  | EmoSN ||   Sentisense |  NRC  | EmoSN")

    for emotion in word_counts:

        emo_counts = word_counts[emotion]

        # percentuali parole conteggiate presenti anche nelle risorse (intersezione/tot conteggi)
        perc_cont_sentisense ="n.d." if emo_counts[4] == 0 else  truncate_to_str(emo_counts[1] * 100 / emo_counts[7])
        perc_cont_nrc = "n.d." if emo_counts[5] == 0 else truncate_to_str(emo_counts[2]  * 100/ emo_counts[8])
        perc_cont_emosn = "n.d." if emo_counts[6] == 0 else truncate_to_str(emo_counts[3] * 100 / emo_counts[9])

        # percentuali parole risorse presenti nei conteggi (intersezione/tot parole risorse)
        perc_sentisense = "n.d." if emo_counts[4] == 0 else truncate_to_str(emo_counts[1]  * 100/ emo_counts[4])
        perc_nrc = "n.d." if emo_counts[5] == 0 else truncate_to_str(emo_counts[2] * 100 / emo_counts[5])
        perc_emosn = "n.d." if emo_counts[6] == 0 else truncate_to_str(emo_counts[3] * 100 / emo_counts[6])

        print(emotion.ljust(12), '|      {} | {} | {} ||   {} | {} | {}  || {}'
            .format(perc_cont_sentisense.ljust(9), perc_cont_nrc.ljust(5), perc_cont_emosn.ljust(5),
                    perc_sentisense.ljust(10), perc_nrc.ljust(5), perc_emosn.ljust(5),
                    str(emo_counts[0])))


def truncate_to_str(n, decimals=1):

    if type(n) == str:
        return n
    else:
        multiplier = 10 ** decimals
        return str(int(n * multiplier) / multiplier)


def plot_counts(setting_data):
    conn = mariadb.connect(
        user=setting_data['Username'],
        password=setting_data['Password'],
        host=setting_data['HostName'],
        port=setting_data['Port'],
        database=setting_data['DatabaseName'])

    cursor = conn.cursor()

    # ottenimento conteggi
    cursor.execute("SELECT COUNT FROM wordcount ORDER BY COUNT")

    x_vett = []
    y_vett = []

    count = 1

    for value in cursor:
        x_vett.append(count)
        count += 1

        y_vett.append(value)

    # plot del grafico
    plt.plot(x_vett, y_vett)
    plt.xlabel("Words")
    plt.ylabel("Count")
    plt.semilogy()
    plt.show()


