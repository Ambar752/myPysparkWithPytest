from pyspark.sql import SparkSession
def getSparkSession(appName):
    return SparkSession.builder.appName(appName).master("local").getOrCreate()
