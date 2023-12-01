from pandas import read_csv
import os

TEST_CVS_NAME = "test_PYTHON_CSV.csv"


def test_write_to_csv(establish_connection):
    outputString = """Alice,[""Aida""],"[10,5]"\nBob,[""Bobby""],"[12,8]"\nCarol,"[""Carmen"",""Fred""]","[4,5]"\nDan,"[""Wolfeschlegelstein"",""Daniel""]","[1,9]"\nElizabeth,[""Ein""],[2]\nFarooq,[""Fesdwe""],"[3,4,5,6,7]"\nGreg,[""Grad""],[1]\nHubert Blaine Wolfeschlegelsteinhausenbergerdorff,"[""Ad"",""De"",""Hi"",""Kye"",""Orlan""]","[10,11,12,3,4,5,6,7]"
"""
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) RETURN a.fName, a.usedNames, a.workedHours")
    result.write_to_csv(TEST_CVS_NAME)
    result.close()
    with open(TEST_CVS_NAME) as csv_file:
        data = csv_file.read()
    assert (data == outputString)
    os.remove(TEST_CVS_NAME)


def test_write_to_csv_extra_args(establish_connection):
    outputString = """35|1~30|2~45|1~20|2~20|1~25|2~40|2~83|2~"""
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) RETURN a.age, a.gender")
    result.write_to_csv(TEST_CVS_NAME, "|", '"', '~')
    result.close()
    with open(TEST_CVS_NAME) as csv_file:
        data = csv_file.read()
    assert (data == outputString)
    os.remove(TEST_CVS_NAME)


def test_pandas_read_csv_extra_args(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) RETURN a.fName, a.workedHours, a.usedNames")
    result.write_to_csv(TEST_CVS_NAME, "|", "'", "~")
    result.close()
    df = read_csv(TEST_CVS_NAME, delimiter="|",
                  lineterminator="~", escapechar="`")
    assert (df.iloc[:, 0].tolist() == ['Bob', 'Carol', 'Dan', 'Elizabeth',
            'Farooq', 'Greg', 'Hubert Blaine Wolfeschlegelsteinhausenbergerdorff'])
    assert (df.iloc[:, 1].tolist() == ['[12,8]', '[4,5]', '[1,9]',
            '[2]', '[3,4,5,6,7]', '[1]', '[10,11,12,3,4,5,6,7]'])
    assert (df.iloc[:, 2].tolist() == ["[''Bobby'']", "[''Carmen'',''Fred'']", "[''Wolfeschlegelstein'',''Daniel'']",
            "[''Ein'']", "[''Fesdwe'']", "[''Grad'']", "[''Ad'',''De'',''Hi'',''Kye'',''Orlan'']"])
    os.remove(TEST_CVS_NAME)
