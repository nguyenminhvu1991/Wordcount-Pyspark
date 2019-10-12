
"""
USE PYSPARK (RUN ON HADOOP HDFS) TO MAKE WORDCOUNT FOR THE BOOK "THE HUNGER GAME"

"""

from pyspark import SparkContext
import pandas as pd
import matplotlib.pyplot as plt
import numpy 

sc = SparkContext(appName="WordCountapp")
#input
input = sc.textFile("D:/SP19/CIS-557_big Data/PROJECT_BIG-DATA/pyspark/The_Hunger_Games.txt")

#Mapping_splitting
map1 = input.flatMap(lambda line: line.split(" "))

#Mapping_Stripping
map2 = map1.map(lambda word: word.strip(" "))\
            .map(lambda word: word.strip(","))\
            .map(lambda word: word.strip("."))\
            .map(lambda word: word.strip("?"))\
            .map(lambda word: word.strip("!"))\
            .map(lambda word: word.strip("@"))\
            .map(lambda word: word.strip("#"))\
            .map(lambda word: word.strip("&"))\
            .map(lambda word: word.strip("*"))\
            .map(lambda word: word.strip(":"))

#Mapping_Key-value pairing
map1 =map2.map(lambda w: (w, 1))
#Reduce
counts = map1.reduceByKey(lambda a1, a2: a1 + a2)\
            .filter(lambda x: x[1]>=500)

output_wordcount = counts.collect()

#dataframe and print
dataframe = pd.DataFrame(data =output_wordcount)
print(dataframe)  

#visualize
x = numpy.array(dataframe[0])
print(x)
y = numpy.array(dataframe[1])
print(y)
plt.figure(figsize=(10, 12))
plt.bar(x,y )



