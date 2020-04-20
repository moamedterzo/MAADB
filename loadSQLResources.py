import os
import pyodbc

from loadMongoDBResources import path_tweet

conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=localhost\SQLEXPRESS;"
                      "Database=TwitterEmotions;"
                      "Trusted_Connection=yes;")
cursor = conn.cursor()


def load_tweet():
    tweets = []
    for filename in os.listdir(path_tweet):
        with open(path_tweet + '\\' + filename, 'r', encoding="utf8") as reader:
            line = reader.readline()
            while line != '':
                cursor.execute("insert into Tweet(Text) values ('" + line.replace("'", "''") + "');")
                line = reader.readline()
            conn.commit()



load_tweet()
