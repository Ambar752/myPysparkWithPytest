from utils.dataUtils import *
from utils.baseUtils import getSparkSession
import pytest
import pytest_mock

@pytest.fixture
def getSession(scope="module"):
    spark = getSparkSession("testSparkSession")
    yield spark
    del spark
@pytest.fixture
def getOrderDF(scope="module"):
    orderDF = getCSVFileData("D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/OrderDetails.csv")
    yield  orderDF
    del orderDF

@pytest.fixture
def getProductDF(scope="module"):
    ProductDF = getCSVFileData("D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/ProductMaster.csv")
    yield  ProductDF
    del ProductDF

#Example of Mocking a Function call and asserting its call frequency
def test_showCSVFileData(mocker,getOrderDF):
    mock_get = mocker.patch("utils.dataUtils.getCSVFileData",return_value=getOrderDF)
    showCSVFileData("D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/OrderDetails.csv")
    mock_get.assert_called_once()

def test_getCSVFileData_forNumberOfRowsOfOrder(getSession,mocker):
    mock_get = mocker.patch('utils.baseUtils.getSparkSession',return_value=getSession)
    expectedValue = getCSVFileData("D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/OrderDetails.csv").count()
    assert expectedValue == 4

#Example of Passed a Fixture which is Scoped at Module Level
def test_getCSVFileData_forSchemaOfOrder(getOrderDF):
    orderSchema = [col.name for col in getOrderDF.schema.fields]
    assert  orderSchema == ["OrderID","OrderDate","ProductID","ProductQuantity","OrderByFullName"]

#Example of Passed a Fixture which is Scoped at Module Level
def test_getCSVFileData_forNumberOfRowsOfProduct(getProductDF):
    assert getProductDF.count() == 3

#Example of Passed a Fixture which is Scoped at Module Level
def test_getCSVFileData_forSchemaOfProduct(getProductDF):
    productSchema = [col.name for col in getProductDF.schema.fields]
    assert productSchema == ["ProductID","ProductName","ProductPrice"]

def test_joinOrdersAndProduct():
    joinedDF = joinOrdersAndProduct("D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/OrderDetails.csv",
                                    "D:/ambar/PycharmProjects/pysparkWithPytest/src/main/data/ProductMaster.csv",
                                    ["ProductID"])
    assert (sorted([col.name for col in joinedDF.schema.fields],key=lambda word : word[-4:]) ==
            sorted(["OrderID","OrderDate","ProductID","ProductQuantity","OrderByFullName","ProductName","ProductPrice"],key=lambda word : word[-4:]))

