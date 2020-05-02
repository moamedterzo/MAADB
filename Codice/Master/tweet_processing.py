import re
from resource_initializer import punctuations
from nltk.stem import WordNetLemmatizer

client_master = None
client_shard = None
db = None


def remove_nick_and_url(line):
    if "USERNAME" in line:
        line = line.replace("USERNAME", " ")
    if "URL" in line:
        line = line.replace("URL", " ")
    return line

def remove_punctuation(line):
    for punctuation in punctuations:
        if punctuation in line:
            line = line.replace(punctuation, "")
    return line

def remove_digits(line):
    return ''.join(" " if c.isdigit() else c for c in line)

def substitute_slangs(line, slang_dict):
    for slang in slang_dict:
        slang = " " + slang + " "
        standard = " " + slang_dict[slang.replace(" ", "")] + " "

        if line.startswith(slang[1::]):
            line = line.replace(slang[1::], standard) + line[len(slang[1::])::]
        if slang in line:
            line = line.replace(slang, standard)
        if line.endswith(slang[::-1]):
            line = line[:-len(slang[1::])] + line.replace(slang[::-1], standard)
    return line


def delete_emoji_and_emoticon(line, emoticon_list):
    for emoticon in emoticon_list:
        if emoticon in line:
            line = line.replace(emoticon, ' ')
    return line


def delete_stopwords(line, stop_word_list):
    for stop_word in stop_word_list:
        stop_word = " " + stop_word + " "

        if line.startswith(stop_word[1::]):
            line = line[len(stop_word[1::])::]
        if stop_word in line:
            line = line.replace(stop_word, " ")
        if line.endswith(stop_word[1:-1]):
            line = line[:-len(stop_word[::])]
    return line


def process_tweet(line, emoticon_list, slang_dict, stop_word_list):

    # Conteggio degli hashtag e delle emoticon
    line_words_emotion_list = []
    line_hashtag_list = []
    line_emoticon_list = []

    for words in line.split():
        if words.startswith('#') and words != '#':
            line_hashtag_list.append(words)
        if words in emoticon_list: #todo se ci sono due emoticon attaccate, queste non vengono prese
            line_emoticon_list.append(words)

    #todo come gestire i negativi?
    line = remove_nick_and_url(line)
    line = delete_emoji_and_emoticon(line, emoticon_list)
    line = remove_punctuation(line)
    line = remove_digits(line)
    line = line.lower()
    line = substitute_slangs(line, slang_dict)

    line = WordNetLemmatizer().lemmatize(line)
    line = delete_stopwords(line, stop_word_list)
    line = re.sub(' +', ' ', line.strip())


    # Count delle parole
    for words in line.split():
        if words not in line_hashtag_list:
            line_words_emotion_list.append(words)

    return line_words_emotion_list, line_emoticon_list, line_hashtag_list

