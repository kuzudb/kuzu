import os

import pytest
import pexpect
from conftest import ShellTest
from test_helper import deleteIfExists


def test_basic(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement('RETURN "databases rule" AS a;')
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")


def test_range(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement("RETURN RANGE(0, 10) AS a;")
    result = test.run()
    result.check_stdout("[0,1,2,3,4,5,6,7,8,9,10]")


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("RETURN LIST_CREATION(1,2);", "[1,2]"),
        ("RETURN STRUCT_PACK(x:=2,y:=3);", "{x: 2, y: 3}"),
        ("RETURN STRUCT_PACK(x:=2,y:=LIST_CREATION(1,2));", "{x: 2, y: [1,2]}"),
    ],
)
def test_nested_types(temp_db, input, output) -> None:
    test = ShellTest().add_argument(temp_db).statement(input)
    result = test.run()
    result.check_stdout(output)


def test_invalid_cast(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("CREATE NODE TABLE a(i STRING, PRIMARY KEY(i));")
        .statement("CREATE (:a {i: '****'});")
        .statement('MATCH (t:a) RETURN CAST(t.i, "INT8");')
    )
    result = test.run()
    result.check_stdout(
        'Error: Conversion exception: Cast failed. Could not convert "****" to INT8.',
    )


def test_multiline(temp_db) -> None:
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
    result.check_stdout("\u2502 databases rule \u2502")


def test_multi_queries_one_line(temp_db) -> None:
    # two successful queries
    test = ShellTest().add_argument(temp_db).statement('RETURN "databases rule" AS a; RETURN "kuzu is cool" AS b;')
    result = test.run()
    print(result.stdout)
    result.check_stdout("\u2502 databases rule \u2502")
    result.check_stdout("\u2502 kuzu is cool \u2502")

    # one success one failure
    test = ShellTest().add_argument(temp_db).statement('RETURN "databases rule" AS a;      ;')
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")
    result.check_stdout(
        [
            "Error: Parser exception: mismatched input '<EOF>' expecting {ALTER, ATTACH, BEGIN, CALL, CHECKPOINT, COMMENT, COMMIT, COPY, CREATE, DELETE, DETACH, DROP, EXPORT, IMPORT, INSTALL, LOAD, MATCH, MERGE, OPTIONAL, PROJECT, RETURN, ROLLBACK, SET, UNWIND, USE, WITH} (line: 1, offset: 6)",
            '"      "',
        ],
    )

    # two failing queries
    test = ShellTest().add_argument(temp_db).statement('RETURN "databases rule" S a;      ;')
    result = test.run()
    result.check_stdout(
        [
            "Error: Parser exception: Invalid input < S>: expected rule ku_Statements (line: 1, offset: 24)",
            '"RETURN "databases rule" S a"',
            "                         ^",
            "Error: Parser exception: mismatched input '<EOF>' expecting {ALTER, ATTACH, BEGIN, CALL, CHECKPOINT, COMMENT, COMMIT, COPY, CREATE, DELETE, DETACH, DROP, EXPORT, IMPORT, INSTALL, LOAD, MATCH, MERGE, OPTIONAL, PROJECT, RETURN, ROLLBACK, SET, UNWIND, USE, WITH} (line: 1, offset: 6)",
            '"      "',
        ],
    )


def test_row_truncation(temp_db, csv_path) -> None:
    test = ShellTest().add_argument(temp_db).statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    result = test.run()
    result.check_stdout("(21 tuples, 20 shown)")
    result.check_stdout(["\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502"])


def test_column_truncation(temp_db, csv_path) -> None:
    # width when running test is 80
    test = ShellTest().add_argument(temp_db).statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    result = test.run()
    result.check_stdout("id")
    result.check_stdout("fname")
    result.check_stdout("Gender")
    result.check_stdout("\u2502 ... \u2502")
    result.check_stdout("courseScoresPerTerm")
    result.check_stdout("(13 columns, 4 shown)")

    # test column name truncation
    long_name = "a" * 100
    test = ShellTest().add_argument(temp_db).statement(f'RETURN "databases rule" AS {long_name};')
    result = test.run()
    result.check_stdout(f"\u2502 {long_name[:73]}... \u2502")
    result.check_stdout("\u2502 databases rule")


def long_messages(temp_db) -> None:
    long_message = "-" * 4096
    test = ShellTest().add_argument(temp_db).statement(f'RETURN "{long_message}" AS a;')
    result = test.run()
    result.check_stdout(long_message)


def test_history_consecutive_repeats(temp_db, history_path) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument("-p")
        .add_argument(history_path)
        .statement('RETURN "databases rule" AS a;')
        .statement('RETURN "databases rule" AS a;')
    )
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")

    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == 'RETURN "databases rule" AS a;\n'
        assert f.readline() == ""

    deleteIfExists(os.path.join(history_path, "history.txt"))


def test_kuzurc(temp_db) -> None:
    deleteIfExists(".kuzurc")
    # confirm that nothing is read on startup
    test = ShellTest().add_argument(temp_db)
    result = test.run()
    result.check_not_stdout("-- Loading resources from .kuzurc")

    # create a .kuzurc file
    with open(".kuzurc", "w") as f:
        f.write("CREATE NODE TABLE a(i STRING, PRIMARY KEY(i));\n")
        f.write(":max_rows 1\n")
    
    # confirm that the file is read on startup
    test = ShellTest().add_argument(temp_db).statement("CALL show_tables() RETURN *;")
    result = test.run()
    result.check_stdout("-- Loading resources from .kuzurc")
    result.check_stdout("maxRows set as 1")
    result.check_stdout("a")

    deleteIfExists(".kuzurc")

    # create a .kuzurc file with errors
    with open(".kuzurc", "w") as f:
        f.write('RETURN "databases rule" S a;      ;\n')
        f.write(":max_rows\n")
        f.write(":mode table\n")
        f.write("CREATE NODE TABLE b(i STRING, PRIMARY KEY(i));\n")

    # confirm that the file is read on startup
    test = ShellTest().add_argument(temp_db).statement("CALL show_tables() RETURN *;")
    result = test.run()
    result.check_stdout("-- Loading resources from .kuzurc")
    result.check_stdout(
        [
            "Error: Parser exception: Invalid input < S>: expected rule ku_Statements (line: 1, offset: 24)",
            '"RETURN "databases rule" S a"',
            "                         ^",
            "Error: Parser exception: mismatched input '<EOF>' expecting {ALTER, ATTACH, BEGIN, CALL, CHECKPOINT, COMMENT, COMMIT, COPY, CREATE, DELETE, DETACH, DROP, EXPORT, IMPORT, INSTALL, LOAD, MATCH, MERGE, OPTIONAL, PROJECT, RETURN, ROLLBACK, SET, UNWIND, USE, WITH} (line: 1, offset: 6)",
            '"      "',
        ],
    )
    result.check_stdout("Cannot parse '' as number of rows. Expect integer.")
    result.check_stdout("mode set as table")
    result.check_stdout("b")

    deleteIfExists(".kuzurc")


def test_comments(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN // testing\n /* test\ntest\ntest */"databases rule" // testing\n AS a\n; // testing')
        .statement('\x1b[A\r') # run the last command again to make sure comments are still ignored
    )
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")


def test_shell_auto_completion(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("CREATE NODE TABLE coolTable(i STRING, longPropertyName STRING, PRIMARY KEY(i));\n")
    )
    result = test.run()
    result.check_stdout("\u2502 Table coolTable has been created. \u2502")

    # table auto completion only works if table exists on startup currently and
    # due to how the shell handles tab characters, in this test we need to send 
    # tabs without characters directly after them to avoid being processed as
    # pasted input
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement("ma\t")
    test.send_statement("\t")
    test.send_statement("\t")
    test.send_statement("\x1b[Z")
    test.send_statement(" (n:\t")
    test.send_statement(") ret\t")
    test.send_statement(" n.\t")
    test.send_statement("\t")
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 n.longPropertyName \u2502", pexpect.EOF]) == 0

    test.send_statement("c\t")
    test.send_statement(" show_t\t")
    test.send_statement("() ret\t")
    test.send_finished_statement(" *;\n")
    assert test.shell_process.expect_exact(["\u2502 coolTable \u2502", pexpect.EOF]) == 0
