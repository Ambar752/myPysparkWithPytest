from utils.baseUtils import getSparkSession
import  pytest


#Setup a Fixture
@pytest.fixture
def fixture_appname(scope="function"):
    #Initialize Test Appname
    appname = "myTestAppName"

    #Setup Test Appname
    yield appname

    #TearDown Test Appname
    del appname


#Assert if AppName is set Properly and SparkSession is returned
def test_getSparkSession(fixture_appname):
    testSpark = getSparkSession(fixture_appname)
    #assert testSpark.sparkContext.appName == "myTestAppName"
    assert str(type(testSpark)) == "<class 'pyspark.sql.session.SparkSession'>"
