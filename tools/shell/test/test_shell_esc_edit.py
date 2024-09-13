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
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_word_left(test, esc) -> None:
    test.send_statement("RETURN AS a;")
    test.send_statement(esc * 3)
    test.send_finished_statement(' "databases rule"\x1b[F\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1bf",
        "\x1b[1;5C",
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_word_right(test, esc) -> None:
    test.send_statement('RETURN "databases a;')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)  # move cursor to front of line
    test.send_statement(esc * 3)
    test.send_finished_statement(' rule" AS\x1b[F\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[H",
        "\x1b[1~",
        "\x1bOH",
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_home(test, esc) -> None:
    test.send_statement('ETURN "databases rule" AS a;')
    test.send_statement(esc)
    test.send_finished_statement("R\x1b[F\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_delete(test) -> None:
    test.send_statement('RETURN "databases rule" AS a;abc')
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.send_statement("\x1b[3~" * 4)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "esc",
    [
        "\x1b[F",
        "\x1b[4~",
        "\x1bOF",
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_end(test, esc) -> None:
    test.send_statement('RETURN "databases rule" AS a')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)  # move cursor to front of line
    test.send_statement(esc)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_next_history_singleline(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_statement("\x1b[A")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


def test_next_history_multiline(temp_db) -> None:
    # multiline up arrow will move up if not on top line
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "shell is fun" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 shell is fun \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool"\r AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_statement("\x1b[A\x1b[A\x1b[A")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0

    # singleline up arrow will move up regardless
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement(':singleline\n')
    test.send_finished_statement('RETURN "shell is fun" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 shell is fun \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool"\r AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_statement("\x1b[A\x1b[A\x1b[A")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 shell is fun \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_prev_history_singleline(test) -> None:
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_statement("\x1b[B")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_prev_history_multiline(test) -> None:
    # multiline down arrow will move down if not on bottom line
    test.send_finished_statement('RETURN "kuzu is cool"\r AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_P.value)  # move up line
    test.send_statement("\x1b[B")
    test.send_statement("\x1b[B")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_left(test) -> None:
    test.send_statement(" a;")
    test.send_statement("\x1b[D" * 4)
    test.send_finished_statement('RETURN "databases rule" AS\x1b[F\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_move_right(test) -> None:
    test.send_statement('RETURN "databases rule" AS a')
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_B.value)  # move cursor to the left
    test.send_statement("\x1b[C" * 4)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
