# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from src.main.utils.baseUtils import getSparkSession
from src.main.utils.dataUtils import getCSVFileData,showCSVFileData,joinOrdersAndProduct
from tranformations.rowTranformations import addFirstNameInOrders
from tranformations.aggregations import getPersonWiseSalesAmount
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    showCSVFileData("src/main/data/OrderDetails.csv")
    showCSVFileData("src/main/data/ProductMaster.csv")
    addFirstNameInOrders("src/main/data/OrderDetails.csv").show()
    joinData = joinOrdersAndProduct("src/main/data/OrderDetails.csv",
                         "src/main/data/ProductMaster.csv",
                         ["ProductID"])
    getPersonWiseSalesAmount(joinData).show()
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
