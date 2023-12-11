#!/usr/bin/python3

#import findspark
#findspark.init('/Users/saikatbasu/Documents/spark-2.4.7-bin-hadoop2.7')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, countDistinct, max, mean, avg
import time

spark = SparkSession.builder.appName('myApp1').getOrCreate()

print ("###### LOADING LINEORDER ##########\n")
dfLO = spark.read.csv('/Users/saikatbasu/Documents/MyData/SSB_CSV/SSB.LINEORDER.csv',inferSchema=True, header=True)
print ("###### SHOWING LINEORDER ##########\n")
dfLO.show()
time.sleep(5)

print ("###### LOADING CUSTOMER ##########\n")
dfCUST = spark.read.csv('/Users/saikatbasu/Documents/MyData/SSB_CSV/SSB.CUSTOMER.csv',inferSchema=True, header=True)
print ("###### SHOWING CUSTOMER ##########\n")
dfCUST.show()
time.sleep(5)

print ("###### LOADING DATES ##########\n")
dfDATES = spark.read.csv('/Users/saikatbasu/Documents/MyData/SSB_CSV/SSB.DATES.csv',inferSchema=True, header=True)
print ("###### SHOWING DATES ##########\n")
dfDATES.show()
time.sleep(5)

print ("###### LOADING PARTS ##########\n")
dfPART = spark.read.csv('/Users/saikatbasu/Documents/MyData/SSB_CSV/SSB.PART.csv',inferSchema=True, header=True)
print ("###### SHOWING PARTS ##########\n")
dfPART.show()
time.sleep(5)

print ("###### LOADING SUPPLIER ##########\n")
dfSUPP = spark.read.csv('/Users/saikatbasu/Documents/MyData/SSB_CSV/SSB.SUPPLIER.csv',inferSchema=True, header=True)
print ("###### SHOWING SUPPLIER ##########\n")
time.sleep(5)

print ("######   JOINING TABLES ##########\n")

df = dfLO.join(dfCUST,dfLO.LO_CUSTKEY == dfCUST.C_CUSTKEY,"left") \
 .join(dfDATES, dfLO.LO_ORDERDATE == dfDATES.D_DATEKEY,"left") \
 .join(dfPART, dfLO.LO_PARTKEY == dfPART.P_PARTKEY,"left") \
 .join(dfSUPP, dfLO.LO_SUPPKEY == dfSUPP.S_SUPPKEY, "left")

print ("####### SCHEMA OF FLATTENED TABLE #######\n")
df.printSchema()

print ("#######  FINALLY RUNNING A GROUP BY AGGREGATE QUERY #######\n")
print ("SELECT C_MKTSEGMENT, C_REGION, sum(LO_QUANTITY), sum(LO_REVENUE) from JOINED DATA_FRAME ORDER BY LO_REVENUE\n")

df.groupBy('C_MKTSEGMENT','C_REGION').sum('LO_QUANTITY','LO_REVENUE').orderBy(sum('LO_REVENUE')).show()


