from pyspark.sql.functions import col, sum, expr

def getPersonWiseSalesAmount(inputDF):
    return inputDF \
        .select("OrderByFullName","ProductQuantity","ProductPrice") \
        .withColumn("TotalSales",expr("CAST(ProductQuantity AS INT)")*expr("CAST(ProductPrice AS INT)")) \
        .groupBy("OrderByFullName") \
        .agg(sum("TotalSales").alias("TotalSales"))

