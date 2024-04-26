from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timedelta
import kuzu
from kuzu import Type
import pandas as pd

def udf_helper(conn, functionName, params, expectedResult):
    plist = ', '.join(map(lambda x: "$" + str(x+1), range(len(params))))
    queryString = f"RETURN {functionName}({plist})"
    result = conn.execute(queryString, {str(i+1) : params[i] for i in range(len(params))})
    assert result.get_next()[0] == expectedResult

def test_udf(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite

    def add5int(x: int) -> int:
        print(x)
        return x + 5
    
    add5IntArgs = ["add5int", add5int]
    
    def add5float(x: float) -> float:
        return x + 5.0
    
    add5FloatArgs = ["add5float", add5float]
    
    def intToString(x: int) -> str:
        return str(x)
    
    intToStringArgs = ["intToString", intToString]
    
    def sumOf7(a, b, c, d, e, f, g) -> int:
        return a+b+c+d+e+f+g

    sumOf7Args = ["sumOf7", sumOf7, [Type.INT64] * 7]
    
    def dateToDatetime(a: date) -> datetime:
        return datetime(a.year, a.month, a.day)
    
    dateToDatetimeArgs = ["dateToDatetime", dateToDatetime]
    
    def datetimeToDate(a: datetime) -> date:
        return a.date()
    
    datetimeToDateArgs = ["datetimeToDate", datetimeToDate, [Type.TIMESTAMP], Type.DATE]
    
    def addToDate(a: datetime, b: timedelta) -> datetime:
        return a + b
    
    addToDateArgs = ["addToDate", addToDate, [Type.TIMESTAMP, Type.INTERVAL], Type.TIMESTAMP]
    
    def concatThreeLists(a: list, b: list, c: list) -> list:
        return a+b+c
    
    concatThreeListsArgs = ["concatThreeLists", concatThreeLists, [Type.LIST] * 3, Type.LIST]
    # todo maxwell: implement nested udf
    
    def mergeMaps(a: dict, b: dict) -> dict:
        return a | b
    
    mergeMapsArgs = ["mergeMaps", mergeMaps, [Type.MAP] * 2, Type.MAP]
    # todo maxwell: implement nested udf

    def selectIfSeven(a: int) -> int:
        return a == 7
    
    selectIfSeven = ["selectIfSeven", selectIfSeven, [Type.INT64], Type.BOOL]
    # todo maxwell: implement select udf

    conn.set_udf(*add5IntArgs)
    conn.set_udf(*add5FloatArgs)
    conn.set_udf(*intToStringArgs)
    conn.set_udf(*sumOf7Args)
    conn.set_udf(*dateToDatetimeArgs)
    conn.set_udf(*datetimeToDateArgs)
    conn.set_udf(*addToDateArgs)

    udf_helper(conn, "add5int", [10], 15)
    udf_helper(conn, "add5int", [9], 14)
    udf_helper(conn, "add5int", [1283], 1288)
    udf_helper(conn, "add5float", [2.2], 7.2)
    udf_helper(conn, "intToString", [123], '123')
    udf_helper(conn, "sumOf7", [1, 2, 3, 4, 5, 6, 7], sum(range(8)))
    udf_helper(conn, "dateToDatetime", [date(2024, 4, 25)], datetime(2024, 4, 25))
    udf_helper(conn, "datetimeToDate", [datetime(2024, 4, 25, 15, 39, 20)], date(2024, 4, 25))
    udf_helper(conn, "addToDate", [datetime(2024, 4, 25, 15, 39, 20), datetime(2025, 5, 26) - datetime(2024, 4, 25, 15, 39, 20)], datetime(2025, 5, 26))


    df = pd.DataFrame({"col": list(range(5000))})
    result = conn.execute("LOAD FROM df RETURN add5int(col) as ans").get_as_df()
    assert list(result['ans']) == list(map(add5int, range(5000)))


