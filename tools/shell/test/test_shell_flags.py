import os

import pytest

from conftest import ShellTest
from test_helper import KUZU_VERSION, deleteIfExists


def test_database_path(temp_db) -> None:
    # no database path
    test = ShellTest()
    result = test.run()
    result.check_stdout("Opened the database under in-memory mode.")

    # valid database path
    test = ShellTest().add_argument(temp_db).statement('RETURN "databases rule" AS a;')
    result = test.run()
    result.check_stdout("databases rule")


@pytest.mark.parametrize(
    "flag",
    ["-h", "--help"],
)
def test_help(temp_db, flag) -> None:
    # database path not needed
    test = ShellTest().add_argument(flag)
    result = test.run()
    result.check_stdout("KuzuDB Shell")
    # with database path
    test = ShellTest().add_argument(temp_db).add_argument(flag)
    result = test.run()
    result.check_stdout("KuzuDB Shell")


@pytest.mark.parametrize(
    "flag",
    [
        "-d",
        "--defaultbpsize",
        "--default_bp_size"
    ],
)
def test_default_bp_size(temp_db, flag) -> None:
    # empty flag argument
    test = ShellTest().add_argument(temp_db).add_argument(flag)
    result = test.run()
    result.check_stderr(
        f"Flag '{flag.replace('-', '')}' requires an argument but received none",
    )

    # flag argument is not a number
    test = ShellTest().add_argument(temp_db).add_argument(flag).add_argument("kuzu")
    result = test.run()
    result.check_stderr("Argument '' received invalid value type 'kuzu'")

    # successful flag
    test = ShellTest().add_argument(temp_db).add_argument(flag).add_argument("1000")
    result = test.run()
    result.check_stdout(f"Opened the database at path: {temp_db} in read-write mode.")


@pytest.mark.parametrize(
    "flag",
    [
        "--nocompression",
        "--no_compression"
    ],
)
def test_no_compression(temp_db, flag) -> None:
    test = ShellTest().add_argument(temp_db).add_argument(flag)
    result = test.run()
    result.check_stdout(f"Opened the database at path: {temp_db} in read-write mode.")


@pytest.mark.parametrize(
    "flag",
    [
        "-r",
        "--readonly",
        "--read_only"
    ],
)
def test_read_only(temp_db, flag) -> None:
    # cannot open an empty database in read only mode so initialize database
    test = ShellTest().add_argument(temp_db)
    result = test.run()
    result.check_stdout(f"Opened the database at path: {temp_db} in read-write mode.")

    # test read only
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .statement('RETURN "databases rule" AS a;')
        .statement("CREATE NODE TABLE a(i STRING, PRIMARY KEY(i));")
        .statement('RETURN "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout(f"Opened the database at path: {temp_db} in read-only mode.")
    result.check_stdout("databases rule")
    result.check_stdout(
        "Error: Cannot execute write operations in a read-only database!",
    )
    result.check_stdout("kuzu is cool")


def test_history_path(temp_db, history_path) -> None:
    # empty flag argument
    test = ShellTest().add_argument(temp_db).add_argument("-p")
    result = test.run()
    result.check_stderr("Flag 'p' requires an argument but received none")

    # invalid path
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument("///////")
    result = test.run()
    result.check_stderr("Invalid path to directory for history file")

    # valid path, file doesn't exist
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument("-p")
        .add_argument(history_path)
        .statement('RETURN "databases rule" AS a;')
    )
    result = test.run()
    result.check_stdout("databases rule")
    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == 'RETURN "databases rule" AS a;\n'

    # valid path, file exists
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument("-p")
        .add_argument(history_path)
        .statement('RETURN "kuzu is cool" AS b;')
    )
    result = test.run()
    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == 'RETURN "databases rule" AS a;\n'
        assert f.readline() == 'RETURN "kuzu is cool" AS b;\n'

    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "flag",
    [
        "-v",
        "--version",
    ],
)
def test_version(temp_db, flag) -> None:
    # database path not needed
    test = ShellTest().add_argument(flag)
    result = test.run()
    result.check_stdout(KUZU_VERSION)
    # with database path
    test = ShellTest().add_argument(temp_db).add_argument(flag)
    result = test.run()
    result.check_stdout(KUZU_VERSION)


@pytest.mark.parametrize(
    "flag",
    [
        "-m",
        "--mode",
    ],
)
def test_mode(temp_db, flag) -> None:
    # test default mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\u2502 a              \u2502 b            \u2502")
    result.check_stdout("\u2502 Databases Rule \u2502 kuzu is cool \u2502")

    # test column mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("column")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a                b")
    result.check_stdout("Databases Rule   kuzu is cool")

    # test csv mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("csv")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a,b")
    result.check_stdout("Databases Rule,kuzu is cool")

    # test box mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("box")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\u2502 a              \u2502 b            \u2502")
    result.check_stdout("\u2502 Databases Rule \u2502 kuzu is cool \u2502")

    # test html mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("html")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("<table>")
    result.check_stdout("<tr>")
    result.check_stdout("<th>a</th><th>b</th>")
    result.check_stdout("</tr>")
    result.check_stdout("<tr>")
    result.check_stdout("<td>Databases Rule</td><td>kuzu is cool</td>")
    result.check_stdout("</tr>")
    result.check_stdout("</table>")

    # test json mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("json")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout('[{"a":"Databases Rule","b":"kuzu is cool"}]')

    # test jsonlines mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("jsonlines")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout('{"a":"Databases Rule","b":"kuzu is cool"}')

    # test latex mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("latex")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\\begin{tabular}{ll}")
    result.check_stdout("\\hline")
    result.check_stdout("a&b\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("Databases Rule&kuzu is cool\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("\\end{tabular}")

    # test line mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("line")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a = Databases Rule")
    result.check_stdout("b = kuzu is cool")
    
    # test list mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("list")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a|b")
    result.check_stdout("Databases Rule|kuzu is cool")

    # test markdown mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("markdown")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("| a              | b            |")
    result.check_stdout("| Databases Rule | kuzu is cool |")

    # test table mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("table")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("| a              | b            |")
    result.check_stdout("| Databases Rule | kuzu is cool |")

    # test tsv mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("tsv")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a\tb")
    result.check_stdout("Databases Rule\tkuzu is cool")

    # test trash mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("trash")
        .statement('RETURN RANGE(0, 10) AS a;')
    )
    result = test.run()
    result.check_not_stdout("[0,1,2,3,4,5,6,7,8,9,10]")

    # test mode info
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stderr(f"Flag '{flag.replace('-', '')}' requires an argument but received none")

    # test invalid mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("invalid")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stderr("Cannot parse 'invalid' as output mode.")
    

@pytest.mark.parametrize(
    "flag",
    [
        "-s",
        "--nostats",
        "--no_stats",
    ],
)
def test_no_stats(temp_db, flag) -> None:
    # test stats off
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_not_stdout("(1 tuple)")
    result.check_not_stdout("(1 column)")
    result.check_not_stdout("Time: ")

    # test stats on
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_stdout("(1 tuple)")
    result.check_stdout("(1 column)")
    result.check_stdout("Time: ")


def test_bad_flag(temp_db) -> None:
    # without database path
    test = ShellTest().add_argument("-b")
    result = test.run()
    result.check_stderr("Flag could not be matched: 'b'")

    test = ShellTest().add_argument("--badflag")
    result = test.run()
    result.check_stderr("Flag could not be matched: badflag")

    # with database path
    test = ShellTest().add_argument(temp_db).add_argument("-b")
    result = test.run()
    result.check_stderr("Flag could not be matched: 'b'")

    test = ShellTest().add_argument(temp_db).add_argument("--badflag")
    result = test.run()
    result.check_stderr("Flag could not be matched: badflag")
