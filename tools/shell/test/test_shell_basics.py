import pytest
from test_helper import *
from conftest import ShellTest


def test_basic(temp_db):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "databases rule" AS a;')
    )
    result = test.run()
    result.check_stdout("databases rule")


def test_range(temp_db):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN RANGE(0, 10) AS a;')
    )
    result = test.run()
    result.check_stdout("[0,1,2,3,4,5,6,7,8,9,10]")


@pytest.mark.parametrize(
    ["input", "output"],
    [
        ("RETURN LIST_CREATION(1,2);", "[1,2]"),
        ("RETURN STRUCT_PACK(x:=2,y:=3);", "{x: 2, y: 3}"),
        ("RETURN STRUCT_PACK(x:=2,y:=LIST_CREATION(1,2));", "{x: 2, y: [1,2]}"),
    ],
)
def test_nested_types(temp_db, input, output):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(input)
    )
    result = test.run()
    result.check_stdout(output)
    

def test_invalid_cast(temp_db):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("CREATE NODE TABLE a(i STRING, PRIMARY KEY(i));")
        .statement("CREATE (:a {i: '****'});")
        .statement('MATCH (t:a) RETURN CAST(t.i, "INT8");')
    )
    result = test.run()
    result.check_stdout('Error: Conversion exception: Cast failed. Could not convert "****" to INT8.')
    

def test_multiline(temp_db):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("RETURN")
        .statement('"databases rule"')
        .statement("AS")
        .statement("a")
        .statement(";")
    )
    result = test.run()
    result.check_stdout("databases rule")
    

def test_multi_queries_one_line(temp_db):
    # two successful queries
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "databases rule" AS a; RETURN "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("databases rule") 
    result.check_stdout("kuzu is cool")
    
    # one success one failure
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "databases rule" AS a;      ;')
    )
    result = test.run()
    result.check_stdout("databases rule")
    result.check_stdout(["Error: Parser exception: mismatched input '<EOF>' expecting {CALL, COMMENT, COPY, EXPORT, DROP, ALTER, BEGIN, COMMIT, COMMIT_SKIP_CHECKPOINT, ROLLBACK, ROLLBACK_SKIP_CHECKPOINT, INSTALL, LOAD, OPTIONAL, MATCH, UNWIND, CREATE, MERGE, SET, DETACH, DELETE, WITH, RETURN} (line: 1, offset: 6)", 
                         '"      "'])
    
    # two failing queries
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "databases rule" S a;      ;')
    )
    result = test.run()
    result.check_stdout(["Error: Parser exception: Invalid input < S>: expected rule ku_Statements (line: 1, offset: 24)", 
                         '"RETURN "databases rule" S a"',
                         "                         ^",
                         "Error: Parser exception: mismatched input '<EOF>' expecting {CALL, COMMENT, COPY, EXPORT, DROP, ALTER, BEGIN, COMMIT, COMMIT_SKIP_CHECKPOINT, ROLLBACK, ROLLBACK_SKIP_CHECKPOINT, INSTALL, LOAD, OPTIONAL, MATCH, UNWIND, CREATE, MERGE, SET, DETACH, DELETE, WITH, RETURN} (line: 1, offset: 6)",
                         '"      "'])


def test_row_truncation(temp_db, csv_path):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples, 20 shown)")
    result.check_stdout(["|  . |", "|  . |", "|  . |"])
    

def test_column_truncation(temp_db, csv_path):
    # width when running test is 80
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    result.check_stdout("-" * 80)
    result.check_not_stdout("-" * 81)
    result.check_stdout("| ... |")
    result.check_stdout("(13 columns, 4 shown)")


def long_messages(temp_db):
    long_message = '-' * 4096
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(f'RETURN "{long_message}" AS a;')
    )
    result = test.run()
    result.check_stdout(long_message)


def test_history_consecutive_repeats(temp_db, history_path):
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument("-p")
        .add_argument(history_path)
        .statement('RETURN "databases rule" AS a;')
        .statement('RETURN "databases rule" AS a;')
    )
    result = test.run()
    result.check_stdout("| databases rule |")
    
    f = open(os.path.join(history_path, "history.txt"))
    assert f.readline() == 'RETURN "databases rule" AS a;\n'
    assert f.readline() == ''
    f.close()
    
    deleteIfExists(os.path.join(history_path, 'history.txt'))
    