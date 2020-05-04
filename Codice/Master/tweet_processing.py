import re
from resource_initializer import punctuations
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet
import emoji

#punctuations
def get_punctuations():
    custom_punctuations = punctuations
    custom_punctuations.append('–')
    custom_punctuations.append('—')
    custom_punctuations.append('–')
    custom_punctuations.append('¿')
    custom_punctuations.append('؟')
    custom_punctuations.append('…')
    custom_punctuations.append('‘')
    custom_punctuations.append('’')
    custom_punctuations.append('“')
    custom_punctuations.append('”')
    custom_punctuations.append('«')
    custom_punctuations.append('»')
    custom_punctuations.append('§')
    custom_punctuations.append('©')
    custom_punctuations.append('*')
    custom_punctuations.append('^')
    custom_punctuations.append('%')
    custom_punctuations.append('#')
    custom_punctuations.append('•')
    custom_punctuations.append('|')
    custom_punctuations.append('~')
    custom_punctuations.append(',')
    custom_punctuations.append('.')

    return custom_punctuations


custom_punctuations = get_punctuations()
lemmatizer = WordNetLemmatizer()

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
    for punctuation in custom_punctuations:
        # l'apice, dato che potrebbe essere parte integrante di una parola, va gestito in maniera differente
        if punctuation != "'":
            line = line.replace(punctuation, " ")

    line = line.replace("' ", " ")
    line = line.replace("'s ", " ")
    line = line.replace(" '", " ")

    #remove digits
    return ''.join(c for c in line if not c.isdigit())


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


def substitute_contractions(line):
    for contraction in contractions:
        line = line.replace(" " + contraction + " ", " " + contractions[contraction] + " ")

    #do a second swipe
    for contraction in contractions:
        line = line.replace(contraction, contractions[contraction])

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


def find_hashtags(line):

    hashtags = []

    for hashtag in re.findall(r"#(\w+)", line):
        hashtags.append(hashtag)
        line = line.replace("#" + hashtag, "")

    return line, hashtags


def find_standard_emojis(line):

    new_line = ""
    emojis = []

    for c in line:
        if c in emoji.UNICODE_EMOJI:
            emojis.append(c)
        else:
            new_line += c

    return line, emojis


def find_emojicons(line, emoticon_list):

    emojicons = []
    for emoticon in emoticon_list:
        while line.find(emoticon) != -1:
            line = line.replace(emoticon, "", 1)
            emojicons.append(emoticon)

    return line, emojicons



def process_tweet(line, emoticon_list, slang_dict, stop_word_list):

    #print(line)

    line = remove_nick_and_url(line)

    #hashtag
    line, line_hashtag_list = find_hashtags(line)

    #emojicons
    line, line_emoticon_list = find_emojicons(line, emoticon_list)
    line, standard_emoji_list = find_standard_emojis(line)

    line_emoticon_list.extend(standard_emoji_list)
    del standard_emoji_list

    line = remove_punctuation(line).lower()

    line = substitute_contractions(line)
    line = substitute_slangs(line, slang_dict)
    line = line.replace("'", " ")

    tokens = word_tokenize(line)
    lemmas = lemmatize_tokens(tokens)

    lemmas = [word for word in lemmas if word not in stop_word_list]

    #print(tokens)
    #print(lemmas)
    #print(line_emoticon_list)
    #print(line_hashtag_list)
    #input()

    return lemmas, line_emoticon_list, line_hashtag_list


tag_dict = {"J": wordnet.ADJ,
            "N": wordnet.NOUN,
            "V": wordnet.VERB,
            "R": wordnet.ADV}


def lemmatize_tokens(tokens):
    """Map POS tag to first character lemmatize() accepts"""
    result = []

    pos_tags = pos_tag(tokens)

    for i in range(len(pos_tags)):
        tag = pos_tags[i]
        wn_pos_tag = tag_dict.get(tag[1][0].upper(), wordnet.NOUN)

        lemma = lemmatizer.lemmatize(tokens[i], wn_pos_tag)
        result.append(lemma)

    return result


#todo gestirla nella tabella
contractions = {
"ain't": "am not",
"aren't": "are not",
"can't": "cannot",
"can't've": "cannot have",
"'cause": "because",
"could've": "could have",
"couldn't": "could not",
"couldn't've": "could not have",
"didn't": "did not",
"doesn't": "does not",
"don't": "do not",
"hadn't": "had not",
"hadn't've": "had not have",
"hasn't": "has not",
"haven't": "have not",
"he'd": "he had",
"he'd've": "he would have",
"he'll": "he will",
"he'll've": "he will have",
"he's": "he is",
"how'd": "how did",
"how'd'y": "how do you",
"how'll": "how will",
"how's": "how is",
"i'd": "i had",
"i'd've": "i would have",
"i'll": "i will",
"i'll've": "i will have",
"i'm": "i am",
"i've": "i have",
"isn't": "is not",
"it'd": "it had",
"it'd've": "it would have",
"it'll": "it will",
"it'll've": "it will have",
"it's": "it is",
"let's": "let us",
"ma'am": "madam",
"mayn't": "may not",
"might've": "might have",
"mightn't": "might not",
"mightn't've": "might not have",
"must've": "must have",
"mustn't": "must not",
"mustn't've": "must not have",
"needn't": "need not",
"needn't've": "need not have",
"o'clock": "of the clock",
"oughtn't": "ought not",
"oughtn't've": "ought not have",
"shan't": "shall not",
"sha'n't": "shall not",
"shan't've": "shall not have",
"she'd": "she had",
"she'd've": "she would have",
"she'll": "she will",
"she'll've": "she will have",
"she's": "she is",
"should've": "should have",
"shouldn't": "should not",
"shouldn't've": "should not have",
"so've": "so have",
"so's": "so is",
"that'd": "that had",
"that'd've": "that would have",
"that's": "that is",
"there'd": "there had",
"there'd've": "there would have",
"there's": "there is",
"they'd": "they had",
"they'd've": "they would have",
"they'll": "they will",
"they'll've": "they will have",
"they're": "they are",
"they've": "they have",
"to've": "to have",
"wasn't": "was not",
"we'd": "we had",
"we'd've": "we would have",
"we'll": "we will",
"we'll've": "we will have",
"we're": "we are",
"we've": "we have",
"weren't": "were not",
"what'll": "what will",
"what'll've": "what will have",
"what're": "what are",
"what's": "what is",
"what've": "what have",
"when's": "when is",
"when've": "when have",
"where'd": "where did",
"where's": "where is",
"where've": "where have",
"who'll": "who will",
"who'll've": "who will have",
"who's": "who is",
"who've": "who have",
"why's": "why is",
"why've": "why have",
"will've": "will have",
"won't": "will not",
"won't've": "will not have",
"would've": "would have",
"wouldn't": "would not",
"wouldn't've": "would not have",
"y'all": "you all",
"y'all'd": "you all would",
"y'all'd've": "you all would have",
"y'all're": "you all are",
"y'all've": "you all have",
"you'd": "you had",
"you'd've": "you would have",
"you'll": "you will",
"you'll've": "you will have",
"you're": "you are",
"you've": "you have"
}

