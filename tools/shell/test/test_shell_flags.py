import os

import pytest

from conftest import ShellTest
from test_helper import KUZU_VERSION, deleteIfExists


def test_database_path(temp_db) -> None:
    # no database path
    test = ShellTest()
    result = test.run()
    result.check_stdout("Opening the database under in-memory mode.")

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
    result.check_stdout(f"Opening the database at path: {temp_db} in read-write mode.")


@pytest.mark.parametrize(
    "flag",
    [
        "--maxdbsize",
        "--max_db_size"
    ],
)
def test_max_db_size(temp_db, flag) -> None:
    # empty flag argument
    test = ShellTest().add_argument(temp_db).add_argument(flag)
    result = test.run()
    result.check_stderr(
        f"Flag '{flag.replace('-', '')}' requires an argument but received none",
    )

    # flag argument is not a number
    test = ShellTest().add_argument(temp_db).add_argument(flag).add_argument("kuzu")
    result = test.run()
    result.check_stderr("Argument 'max_db_size' received invalid value type 'kuzu'")

    # invalid flag
    test = ShellTest().add_argument(temp_db).add_argument(flag).add_argument("1000")
    result = test.run()
    result.check_stderr(f"Buffer manager exception: The given max db size should be at least 8388608 bytes.")

    # successful flag
    test = ShellTest().add_argument(temp_db).add_argument(flag).add_argument("8388608")
    result = test.run()
    result.check_stdout(f"Opening the database at path: {temp_db} in read-write mode.")


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
    result.check_stdout(f"Opening the database at path: {temp_db} in read-write mode.")


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
    result.check_stdout(f"Opening the database at path: {temp_db} in read-write mode.")

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
    result.check_stdout(f"Opening the database at path: {temp_db} in read-only mode.")
    result.check_stdout("databases rule")
    result.check_stdout(
        "Error: Connection exception: Cannot execute write operations in a read-only database!",
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

    # test csv escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("csv")
        .statement('RETURN "This is a \\"test\\", with commas, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with commas, ""quotes"", and\nnewlines."')

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

    # test html escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("html")
        .statement('RETURN "This is a <test> & \\"example\\" with \'special\' characters." AS a;')
    )
    result = test.run()
    result.check_stdout("<table>")
    result.check_stdout("<tr>")
    result.check_stdout("<th>a</th>")
    result.check_stdout("</tr>")
    result.check_stdout("<tr>")
    result.check_stdout(
        "<td>This is a &lt;test&gt; &amp; &quot;example&quot; with &apos;special&apos; characters.</td>")
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
    result.check_stdout('[\n{"a":"Databases Rule","b":"kuzu is cool"}\n]')

    # test json escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("json")
        .statement('RETURN "This is a \\"test\\" with backslashes \\\\, newlines\n, and tabs \t." AS a;')
    )
    result = test.run()
    result.check_stdout('[\n{"a":"This is a \\"test\\" with backslashes \\\\, newlines\\n, and tabs \\t."}\n]')

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

    # test jsonlines escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("jsonlines")
        .statement('RETURN "This is a \\"test\\" with backslashes \\\\, newlines\n, and tabs \t." AS a;')
    )
    result = test.run()
    result.check_stdout('{"a":"This is a \\"test\\" with backslashes \\\\, newlines\\n, and tabs \\t."}')

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

    # test latex escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("latex")
        .statement('RETURN "This is a test with special characters: %, $, &, #, _, {, }, ~, ^, \\\\, <, and >." AS a;')
    )
    result = test.run()
    result.check_stdout("\\begin{tabular}{l}")
    result.check_stdout("\\hline")
    result.check_stdout("a\\\\")
    result.check_stdout("\\hline")
    result.check_stdout(
        "This is a test with special characters: \\%, \\$, \\&, \\#, \\_, \\{, \\}, \\textasciitilde{}, \\textasciicircum{}, \\textbackslash{}, \\textless{}, and \\textgreater{}.\\\\")
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

    # test list escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("list")
        .statement('RETURN "This is a \\"test\\", with vertical bars |, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with vertical bars |, ""quotes"", and\nnewlines."')

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

    # test tsv escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument("tsv")
        .statement('RETURN "This is a \\"test\\", with tabs \t, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with tabs \t, ""quotes"", and\nnewlines."')

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


@pytest.mark.parametrize(
    "flag",
    [
        "-b",
        "--no_progress_bar",
        "--noprogressbar"
    ],
)
def test_no_progress_bar(temp_db, flag) -> None:
    # progress bar on by default
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("CALL current_setting('progress_bar') RETURN *;")
    )
    result = test.run()
    result.check_stdout("True")

    # progress bar off
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .statement("CALL current_setting('progress_bar') RETURN *;")
    )
    result = test.run()
    print(result.stdout)
    print(result.stderr)
    result.check_stdout("False")


@pytest.mark.parametrize(
    "flag",
    [
        "-i",
        "--init",
    ],
)
def test_init(temp_db, init_path, flag) -> None:
    # without init
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("CALL show_tables() RETURN *;")
    )
    result = test.run()
    result.check_not_stdout("person")
    result.check_not_stdout("organisation")
    result.check_not_stdout("movies")
    result.check_not_stdout("knows")
    result.check_not_stdout("studyAt")
    result.check_not_stdout("workAt")
    result.check_not_stdout("meets")
    result.check_not_stdout("marries")

    # with init
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument(flag)
        .add_argument(init_path)
        .statement("CALL show_tables() RETURN *;")
    )
    result = test.run()
    result.check_stdout("person")
    result.check_stdout("organisation")
    result.check_stdout("movies")
    result.check_stdout("knows")
    result.check_stdout("studyAt")
    result.check_stdout("workAt")
    result.check_stdout("meets")
    result.check_stdout("marries")


def test_bad_flag(temp_db) -> None:
    # without database path
    test = ShellTest().add_argument("-a")
    result = test.run()
    result.check_stderr("Flag could not be matched: 'a'")

    test = ShellTest().add_argument("--badflag")
    result = test.run()
    result.check_stderr("Flag could not be matched: badflag")

    # with database path
    test = ShellTest().add_argument(temp_db).add_argument("-a")
    result = test.run()
    result.check_stderr("Flag could not be matched: 'a'")

    test = ShellTest().add_argument(temp_db).add_argument("--badflag")
    result = test.run()
    result.check_stderr("Flag could not be matched: badflag")
