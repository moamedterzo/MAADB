from collections import defaultdict
from wordcloud import WordCloud
import numpy as np
from PIL import Image
import string


def print_cloud(frequency_list, path, width=1000, height=1000, flag_emoticons=False):
    twitter_mask = np.array(Image.open("resources/twitter_logo.png"))

    # the regex used to detect words is a combination of normal words, ascii art, and emojis
    # 2+ consecutive letters (also include apostrophes), e.x It'
    normal_word = r"(?:\w[\w']+)"

    # 2+ consecutive punctuations, e.x. :)
    ascii_art = r"(?:[{punctuation}][{punctuation}]+)".format(punctuation=string.punctuation)

    # a single character that is not alpha_numeric or other ascii printable
    emoji = r"(?:[^\s])(?<![\w{ascii_printable}])".format(ascii_printable=string.printable)
    regexp = r"{normal_word}|{ascii_art}|{emoji}".format(normal_word=normal_word, ascii_art=ascii_art,
                                                         emoji=emoji)

    wordcloud = WordCloud(background_color="white", mask=twitter_mask, width=width, height=height,
                          font_path="resources/Symbola.ttf", regexp=regexp,
                          prefer_horizontal=1 if flag_emoticons else 0.9)

    # generazione wordcloud
    wordcloud.generate_from_frequencies(frequency_list)
    wordcloud.to_file("output_clouds/" + path + ".png")


def make_clouds(words_count, hashtags_count, emojicons_count):
    word_hashtag_by_emo = {}
    counts = {}

    # parole (attenzione al raggruppamento)
    for word in words_count:
        # filtro quelle parole che compaiono in emozioni contrapposte
        filtered_emotions = check_word_emotions(words_count[word])

        for emotion in filtered_emotions:
            if emotion not in word_hashtag_by_emo:
                word_hashtag_by_emo[emotion] = {}
                counts[emotion] = 0

            count = words_count[word][emotion]

            counts[emotion] += count
            word_hashtag_by_emo[emotion][word] = count

    # hashtag
    for hashtag in hashtags_count:
        emotion = hashtag['Emotion']
        if emotion not in word_hashtag_by_emo:
            word_hashtag_by_emo[emotion] = {}
            counts[emotion] = 0

        count = hashtag['Count']
        hashtag_label = "#" + hashtag['Hashtag']

        counts[emotion] += count
        word_hashtag_by_emo[emotion][hashtag_label] = count

    # divisione per il totale e generazione
    for emotion in word_hashtag_by_emo:
        for occourrence in word_hashtag_by_emo[emotion]:
            word_hashtag_by_emo[emotion][occourrence] /= counts[emotion]

        print_cloud(word_hashtag_by_emo[emotion], "parole_hashtag_" + emotion)
        print("Generata word cloud delle parole e degli hashtag per l'emozione " + emotion)

    # emoticons ed emoji
    emojicons_result = defaultdict(dict)
    counts = {}
    for emojicon in emojicons_count:
        emotion = emojicon['Emotion']
        count = emojicon['Count']
        code = emojicon['Code']

        if emotion not in emojicons_count:
            counts[emotion] = 0

        counts[emotion] += count

        emojicons_result[emotion][code] = count

    # divisione per il totale e generazione
    for emotion in emojicons_result:
        for emoji in emojicons_result[emotion]:
            emojicons_result[emotion][emoji] /= counts[emotion]

        print_cloud(emojicons_result[emotion], "emojicon_" + emotion, flag_emoticons=True)
        print("Generata word cloud delle emoticons e delle emoji per l'emozione " + emotion)


contrary_emotions = {
    "trust": "disgust",
    "disgust": "trust",
    "joy": "sadness",
    "sadness": "joy",
    "surprise": "anticipation",
    "anticipation": "surprise",
    "fear": "anger",
    "anger": "fear",
}


def check_word_emotions(word_emotions_count):
    result_emotions = []

    for emotion in word_emotions_count:
        contrary_emotion = contrary_emotions[emotion]
        if contrary_emotion in result_emotions:
            # controllo i conteggi delle emozioni
            # se sono sullo stesso ordine di grandezza allora le scarto entrambi
            count_emotion = word_emotions_count[emotion]
            count_contrary_emotion = word_emotions_count[contrary_emotion]

            # se il valore più basso è almeno il 20% del valore più alto, allora scarto le emozioni
            if min(count_emotion, count_contrary_emotion) * 2 > max(count_emotion, count_contrary_emotion):
                result_emotions.remove(contrary_emotion)
                continue

        # aggiunta risultato
        result_emotions.append(emotion)

    return result_emotions
