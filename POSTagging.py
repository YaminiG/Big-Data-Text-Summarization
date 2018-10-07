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
df = spark.read.json("/share_dir/sample_output/cleaned/cleanRecordsDups.json/part-00000-82fe1e7e-73d2-43f5-8842-e1cdbe1564ff-c000.json")

df.describe()
sentences=df.select("text").collect()
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

from collections import Counter
c = Counter(w for w in word_tokens)
n = 500
print(c.most_common(n))
nltk.download('stopwords')
text = str
text = text.lower()
text = re.sub(r'[^\w\s]','',text)
custom_stopwords = ['would', 'could','said','u']

stopWords = set(stopwords.words('english'))
words = word_tokenize(text)
wordsFiltered = []
for w in words:
    if w not in stopWords and w not in string.punctuation and w not in custom_stopwords:
        wordsFiltered.append(w)

nltk.download('wordnet')
wnl = nltk.WordNetLemmatizer()
lemma_words = [wnl.lemmatize(t) for t in wordsFiltered]
from collections import Counter
c = Counter(w for w in wordsFiltered)
n = 500
posList=[]
temp = ''
for i in c.most_common(n):
    # print(i[0])
    temp = nltk.word_tokenize(i[0])
    tagged = nltk.tag.pos_tag(temp)
    print(tagged)
    posList.append(tagged)


print(posList)
