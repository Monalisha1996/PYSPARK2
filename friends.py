
from pyspark import SparkContext
from pyspark.sql import Row,functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#sc = SparkContext("local", "count app")


spark = SparkSession.builder.appName('FRIENDS').getOrCreate()
print('Session created')
friends = spark.read.csv("file:///C:/spark-3.0.0-preview2-bin-hadoop2.7/pyspark-tutorial-master/fakefriends.csv",header=True)

def count(friends):
    total_friends = friends.count()
    return total_friends

def sorting(final_df):
    sort_by_score = final_df.sort('score',ascending=False)
    return sort_by_score

total = count(friends)
print("TOTAL FRIENDS:",total)

#dataframe casting
friends_new = friends.withColumn("SCORE", friends.SCORE.cast('int'))
#sort data as per score
sort_by_score = sorting(friends_new)
#Top 10
top_ten = sort_by_score.take(10)
spark.createDataFrame(top_ten).show()
