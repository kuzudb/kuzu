import os

import pytest
from conftest import ShellTest
from test_helper import KUZU_VERSION, deleteIfExists

def check_fails_without_db(flag, arg=None):
    test = ShellTest().add_argument(flag)
    if arg:
        test.add_argument(arg)
    result = test.run()
    result.check_stderr("Option 'databasePath' is required")


def test_database_path(temp_db) -> None:
    # no database path
    test = ShellTest().statement('RETURN "databases rule" AS a;')
    result = test.run()
    result.check_stderr("Option 'databasePath' is required")

    # invalid database path
    test = ShellTest().add_argument("///////").statement('RETURN "databases rule" AS a;')
    result = test.run()
    result.check_stderr("Cannot open file ///////.lock: Permission denied")

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
    # fails without db path
    check_fails_without_db(flag, "1000")

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
    # fails without db path
    check_fails_without_db(flag)
    
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

    # fails without db path
    check_fails_without_db(flag)
    
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
    # fails without db path
    check_fails_without_db("-p", history_path)

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
