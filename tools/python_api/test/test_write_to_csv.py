def test_write_to_csv(establish_connection):
    outputString = """Alice,[""Aida""],"[10,5]"\nBob,[""Bobby""],"[12,8]"\nCarol,"[""Carmen"",""Fred""]","[4,5]"\nDan,"[""Wolfeschlegelstein"",""Daniel""]","[1,9]"\nElizabeth,[""Ein""],[2]\nFarooq,[""Fesdwe""],"[3,4,5,6,7]"\nGreg,[""Grad""],[1]\nHubert Blaine Wolfeschlegelsteinhausenbergerdorff,"[""Ad"",""De"",""Hi"",""Kye"",""Orlan""]","[10,11,12,3,4,5,6,7]"
""" 
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) RETURN a.fName, a.usedNames, a.workedHours")
    result.writeToCSV("test_PYTHON_CSV.csv")
    with open("test_PYTHON_CSV.csv") as csv_file:
         data = csv_file.read()
    assert(data == outputString)  
