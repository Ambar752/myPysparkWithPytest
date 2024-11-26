from tranformations.rowTranformations import addFirstNameInOrders

def test_addFirstNameInOrders():
    orderDFWithAddedColumn = addFirstNameInOrders("D:/ambar/PycharmProjects/pysparkWithPytest/src/test/data/OrderDetails.csv").select("OrderByFirstName").collect()
    orderListOfAddedColumn = sorted(list(set([name[0] for name in orderDFWithAddedColumn])))
    expectedList = sorted(list(set(["Ramswarth","Aashish","Rakesh"])))
    assert expectedList == orderListOfAddedColumn