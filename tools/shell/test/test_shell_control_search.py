import os

import pexpect
import pytest

from conftest import ShellTest
from test_helper import KEY_ACTION, deleteIfExists


def set_up_search(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "the shell is fun" AS c;\r')
    assert test.shell_process.expect_exact(["\u2502 the shell is fun \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.ENTER.value,
        "\n",
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_enter(test, key) -> None:
    set_up_search(test)
    test.send_statement("databases")
    test.send_finished_statement(key)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_N.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search_next_once(test, key, history_path) -> None:
    # test search next once
    set_up_search(test)
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 the shell is fun \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_N.value,
    ],
)
@pytest.mark.parametrize('mode', [':multiline\n', ':singleline\n'])
def test_search_next_top(temp_db, mode, key, history_path) -> None:
    # test search next until top of history
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    test.send_finished_statement('RETURN "databases are really cool" AS a;\r')
    test.send_finished_statement(mode)
    set_up_search(test)
    for _ in range(6):
        test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases are really cool \u2502", pexpect.EOF]) == 0
    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_S.value,
        KEY_ACTION.CTRL_P.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search_prev_once(test, key) -> None:
    # test search prev once
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 the shell is fun \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_S.value,
        KEY_ACTION.CTRL_P.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search_prev_bottom(test, key) -> None:
    # test search prev until bottom of history
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "the shell is fun" AS c;\r')
    assert test.shell_process.expect_exact(["\u2502 the shell is fun \u2502", pexpect.EOF]) == 0
    test.send_statement('RETURN "searching history" AS d;')
    # start search and move further into history
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_R.value)
    # move back to bottom of history
    for _ in range(3):
        test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 searching history \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_a(test) -> None:
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
    test.send_control_statement(KEY_ACTION.CTRL_A.value)
    test.send_statement("RETURN ")
    test.send_statement("\x1b[F") # move cursor to end
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_tab(test) -> None:
    # test tab becomes space when part of pasted input
    set_up_search(test)
    test.send_statement("databases\trule")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_tab_complete(test) -> None:
    # test tab complete search and go to end of line
    test.send_finished_statement('RETURN databases;\r')
    assert (
        test.shell_process.expect_exact(
            ['Error: Binder exception: Variable databases is not in scope.', pexpect.EOF],
        )
        == 0
    )
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(KEY_ACTION.BACKSPACE.value)  # remove semicolon
    test.send_statement("\x1b[D" * 9)
    test.send_statement('"\x1b[F')
    test.send_finished_statement(' rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_e(test) -> None:
    test.send_finished_statement('RETURN ;\r')
    assert (
        test.shell_process.expect_exact(
            ['Error: Parser exception: Invalid input <RETURN ;>: expected rule oC_RegularQuery (line: 1, offset: 7)',
             pexpect.EOF],
        )
        == 0
    )
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("return")
    test.send_control_statement(KEY_ACTION.CTRL_E.value)
    test.send_statement(KEY_ACTION.BACKSPACE.value)  # remove semicolon
    test.send_finished_statement('"databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_b(test) -> None:
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databasesrule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databasesr")
    test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.send_finished_statement(" \x1b[F\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_f(test) -> None:
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databasesrule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("database")
    test.send_control_statement(KEY_ACTION.CTRL_F.value)
    test.send_finished_statement(" \x1b[F\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_t(test) -> None:
    test.send_finished_statement('RETURN "database srule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 database srule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("database ")
    test.send_control_statement(KEY_ACTION.CTRL_T.value)
    test.send_finished_statement("\x1b[F\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_u_cleared(test) -> None:
    # check if line was cleared
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_finished_statement('RETURN "kuzu is cool" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_u_position(test) -> None:
    # check if position in history was maintained
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("is cool")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_finished_statement("\x1b[A\r")  # move up in history
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_k(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS aabc;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("As a")
    test.send_control_statement(KEY_ACTION.CTRL_K.value)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_d_eof(test) -> None:
    # ctrl d accepts search and acts as EOF when line is empty
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    # search gets accepted
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
    # acts as EOF
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_d_delete(test) -> None:
    # test accepts search and delete
    test.send_finished_statement('RETURN "databases/ rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases/ rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    test.send_finished_statement("\x1b[F\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_l_empty(test) -> None:
    # clear screen with empty line
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_statement("databases")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_l_non_empty(test) -> None:
    # clear screen with non empty line
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_cancel_search_cleared(test, key) -> None:
    # check if line is cleared
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(key)
    test.send_finished_statement('RETURN "kuzu is cool" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_cancel_search_position(test, key) -> None:
    # check if position in history was maintained
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("is cool")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_statement("\x1b[B")  # move down in history
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_H.value,
        KEY_ACTION.BACKSPACE.value,
        KEY_ACTION.CTRL_W.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_backspace(test, key) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databasesabc")
    test.send_statement(key * 3)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
