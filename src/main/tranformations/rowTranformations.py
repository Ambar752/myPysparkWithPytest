from utils.dataUtils import getCSVFileData
from pyspark.sql.functions import split,col

def addFirstNameInOrders(path):
    orderDF = getCSVFileData(path)
    return orderDF.withColumn("OrderByFirstName",split(col("OrderByFullName")," ")[0])