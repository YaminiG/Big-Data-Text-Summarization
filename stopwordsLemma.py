%sh
cd ..
cd share_dir
ls

%sh
pip install justext

%spark.pyspark
import justext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
path = "/share_dir/14_small_sentences.json"
htmlDF = spark.read.json(path)
def HtmlClean(rawtext):
    paragraphs = justext.justext(rawtext,justext.get_stoplist("English"))
    return ' '.join([p.text for p in paragraphs if not p.is_boilerplate])
pysparkClean = udf(HtmlClean, StringType())
cleanDF = htmlDF.withColumn('cleantext',pysparkClean(htmlDF.text)).drop(htmlDF.text).withColumnRenamed('cleantext','text')
#exports the text:
cleanDF.coalesce(1).write.format('json').save('/share_dir/sample_output/cleaned/cleanRecordsdups.json')

%sh
cd ..
cd share_dir

cd sample_output
cd cleaned/cleanRecordsDups.json/
ls

%spark.pyspark
df = spark.read.json("/share_dir/sample_output/cleaned/cleanRecordsDups.json/part-00000-82fe1e7e-73d2-43f5-8842-e1cdbe1564ff-c000.json")
df.describe()

%spark.pyspark
sentences=df.select("text").collect()

%spark.pyspark
print(sentences[4])
import re
#df = spark.read.json("/share_dir/14_small_sentences.json")
#sentences = df.select("summary").collect()
j = 0
str = ""
strNew=""
for i in df.collect():
   if (i.text):
       str += i.text
      # strNew+=re.sub(r"https\S+","",str)
   j += 1
print(j)
print(str)

import json
import nltk
import string
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
#from wordcloud import WordCloud, STOPWORDS
import numpy as np
import matplotlib.pyplot as plt
word_tokens = word_tokenize(str)
word_tokens =[w.lower() for w in word_tokens]
print(word_tokens)
nltk.download('stopwords')
stopWords = set(stopwords.words('english'))
words = word_tokenize(text)
wordsFiltered = []
for w in words:
    if w not in stopWords and w not in string.punctuation:
        wordsFiltered.append(w)


nltk.download('wordnet')
wnl = nltk.WordNetLemmatizer()
lemma_words = [wnl.lemmatize(t) for t in wordsFiltered]
