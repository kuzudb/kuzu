import pexpect
import pytest
from conftest import ShellTest
from test_helper import KEY_ACTION


@pytest.mark.parametrize(
    "esc",
    [
        "\x1bb",
        "\x1b[1;5D",
    ],
)
def test_move_word_left(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement("RETURN AS a;")
    test.send_statement(esc * 3)
    test.send_finished_statement(' "databases rule"\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1bf",
        "\x1b[1;5C",
    ],
)
def test_move_word_right(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases a;')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)  # move cursor to front of line
    test.send_statement(esc * 3)
    test.send_finished_statement(' rule" AS\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[H",
        "\x1b[1~",
        "\x1bOH",
    ],
)
def test_move_home(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('ETURN "databases rule" AS a;')
    test.send_statement(esc)
    test.send_finished_statement("R\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_delete(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;abc')
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.send_statement("\x1b[3~" * 4)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[F",
        "\x1b[4~",
        "\x1bOF",
    ],
)
def test_move_end(temp_db, esc) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)  # move cursor to front of line
    test.send_statement(esc)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_next_history(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_statement("\x1b[A")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_prev_history(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_statement("\x1b[B")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_move_left(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement(" a;")
    test.send_statement("\x1b[D" * 4)
    test.send_finished_statement('RETURN "databases rule" AS\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_move_right(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a')
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_B.value)  # move cursor to the left
    test.send_statement("\x1b[C" * 4)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
