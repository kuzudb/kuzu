import pexpect
import pytest
from conftest import ShellTest
from test_helper import KEY_ACTION


def set_up_search(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "the shell is fun" AS c;\r')
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)


def test_accept_search(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases" AS a;\r')
    assert test.shell_process.expect_exact(["| databases |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_statement("\x1b\x1b")
    test.send_finished_statement(" rule\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[H",
        "\x1b[1~",
        "\x1bOH",
    ],
)
def test_accept_move_home(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('"databases rule" AS a;\r')
    assert (
        test.shell_process.expect_exact(
            [
                "Error: Parser exception: extraneous input '\"databases rule\"'",
                pexpect.EOF,
            ],
        )
        == 0
    )
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_statement(esc)
    test.send_statement("RETURN ")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[4~",
        "\x1b[8~",
        "\x1b[F",
        "\x1bOF",
    ],
)
def test_accept_move_end(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases ;\r')
    assert (
        test.shell_process.expect_exact(
            ['Error: Parser exception: Invalid input <RETURN ">:', pexpect.EOF],
        )
        == 0
    )
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_statement(esc)
    test.send_statement(KEY_ACTION.BACKSPACE.value)  # remove semicolon
    test.send_finished_statement('rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_next_history(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("the shell is fun")
    test.send_statement("\x1b[A" * 2)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_prev_history(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_statement("\x1b[B" * 2)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0


def test_move_left(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["| databasesrule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databasesr")
    test.send_statement("\x1b[D")
    test.send_finished_statement(" \r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_move_right(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["| databasesrule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("database")
    test.send_statement("\x1b[C")
    test.send_finished_statement(" \r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
