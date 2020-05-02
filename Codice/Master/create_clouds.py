from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import pandas as pd
import json


def cloud():
    stopwords = set(STOPWORDS)

    with open('aa.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        print(data)
        hashtag = {}
        totale = 0
        for value in data['values']:
            hashtag.update({value['hashtag']: int(value['count']['$numberInt'])})
            # print('Hashtag: ' + str(value['hashtag']) +' '+ str(value['count']['$numberInt']))
            totale += int(value['count']['$numberInt'])
        for key in hashtag:
            hashtag.update({key: int(hashtag[key]) / totale})
        '''
        wordcloud = WordCloud(width=1000, height=1000,
                              background_color='white',
                              stopwords=stopwords,
                              min_font_size=10).generate_from_frequencies(hashtag)
        '''
        wordcloud = WordCloud(background_color='white',width=1000, height=1000,
                              stopwords=stopwords,
                            ).generate_from_frequencies(hashtag)
        wordcloud.to_file("joy.png")
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.show()

cloud()
