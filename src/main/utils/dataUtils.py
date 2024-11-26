import os
from utils.baseUtils import getSparkSession

def getCSVFileData(path):
    spark = getSparkSession("myApp")
    return spark.read.option("header","true").csv(path)

def showCSVFileData(path):
    getCSVFileData(path).show(truncate=False)

def joinOrdersAndProduct(ordersPath,ProductPath,joinKeyList):
    ordersData  = getCSVFileData(ordersPath)
    productData = getCSVFileData(ProductPath)
    return ordersData.join(productData,on=joinKeyList,how="inner")

