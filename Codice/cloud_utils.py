from collections import defaultdict
from wordcloud import WordCloud, STOPWORDS
import numpy as np
from PIL import Image
import string

def print_cloud(frequency_list, path, width, height, flag_emoticons=False):
    twitter_mask = np.array(Image.open("resources/twitter_logo.png"))
    stopwords = set(STOPWORDS)

    # the regex used to detect words is a combination of normal words, ascii art, and emojis
    # 2+ consecutive letters (also include apostrophes), e.x It'
    normal_word = r"(?:\w[\w']+)"
    # 2+ consecutive punctuations, e.x. :)
    ascii_art = r"(?:[{punctuation}][{punctuation}]+)".format(punctuation=string.punctuation)
    # a single character that is not alpha_numeric or other ascii printable
    emoji = r"(?:[^\s])(?<![\w{ascii_printable}])".format(ascii_printable=string.printable)
    regexp = r"{normal_word}|{ascii_art}|{emoji}".format(normal_word=normal_word, ascii_art=ascii_art,
                                                         emoji=emoji)

    wordcloud = WordCloud(background_color="white", mask=twitter_mask, width=width, height=height, stopwords=stopwords,
                          font_path="resources/Symbola.ttf", regexp=regexp, prefer_horizontal=1 if flag_emoticons else 0.9)

    wordcloud.generate_from_frequencies(frequency_list)

    wordcloud.to_file("output_clouds/" + path + ".png")


def make_clouds(words_count, hashtags, emojicons_count):
    '''
    for emotion in words_count:
        words = {}
        total = 0
        for word in words_count[emotion]:
            if int(word['Count']) >= 5:
                words.update({word['_id']['Word']: int(word['Count'])})
                total += int(word['Count'])
        for key in words:
            words.update({key: int(words[key]) / total})

        print_cloud(words, "words_" + emotion, 1000, 1000)
        print("Generata word cloud delle parole per l'emozione " + emotion)
    '''

    emojicons_result = {}
    counts = {}
    for emojicon in emojicons_count:
        emotion = emojicon['Emotion']
        if emotion not in emojicons_count:
            emojicons_result[emotion] = {}
            counts[emotion] = 0

        count = emojicon['Count']

        counts[emotion] += count
        emojicons_result[emotion][emojicon['Code']] = count

    # divisione per il totale
    for emotion in emojicons_result:
        for emoji in emojicons_result[emotion]:
            emojicons_result[emotion][emoji] /= counts[emotion]

        print_cloud(emojicons_result[emotion], "emojicon_" + emotion, 1000, 1000, flag_emoticons=True)
        print("Generata word cloud delle emoticons e delle emoji per l'emozione " + emotion)


