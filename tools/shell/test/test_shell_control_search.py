import os

import pexpect
import pytest
from conftest import ShellTest
from test_helper import KEY_ACTION, deleteIfExists


def set_up_search(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "the shell is fun" AS c;\r')
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.ENTER.value,
        "\n",
    ],
)
def test_enter(temp_db, key) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_statement("databases")
    test.send_finished_statement(key)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_N.value,
    ],
)
def test_search_next(temp_db, key, history_path) -> None:
    # test search next once
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0

    # test search next until top of history
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    set_up_search(test)
    for _ in range(4):
        test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_S.value,
        KEY_ACTION.CTRL_P.value,
    ],
)
def test_search_prev(temp_db, key) -> None:
    # test search prev once
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0

    # test search prev until bottom of history
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "the shell is fun" AS c;\r')
    assert test.shell_process.expect_exact(["| the shell is fun |", pexpect.EOF]) == 0
    test.send_statement('RETURN "searching history" AS d;')
    # start search and move further into history
    for _ in range(3):
        test.send_control_statement(KEY_ACTION.CTRL_R.value)
    # move back to bottom of history
    for _ in range(3):
        test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| searching history |", pexpect.EOF]) == 0


def test_ctrl_a(temp_db) -> None:
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
    test.send_control_statement(KEY_ACTION.CTRL_A.value)
    test.send_statement("RETURN ")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_tab(temp_db) -> None:
    # test tab becomes space when part of pasted input
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_statement("databases\trule")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0

    # test tab complete search and go to end of line
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
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(KEY_ACTION.BACKSPACE.value)  # remove semicolon
    test.send_finished_statement('rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_e(temp_db) -> None:
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
    test.send_control_statement(KEY_ACTION.CTRL_E.value)
    test.send_statement(KEY_ACTION.BACKSPACE.value)  # remove semicolon
    test.send_finished_statement('rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_b(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["| databasesrule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databasesr")
    test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.send_finished_statement(" \r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_f(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databasesrule" AS a;\r')
    assert test.shell_process.expect_exact(["| databasesrule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("database")
    test.send_control_statement(KEY_ACTION.CTRL_F.value)
    test.send_finished_statement(" \r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_t(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "database srule" AS a;\r')
    assert test.shell_process.expect_exact(["| database srule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("database ")
    test.send_control_statement(KEY_ACTION.CTRL_T.value)
    test.send_finished_statement("\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_u(temp_db) -> None:
    # check if line was cleared
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_finished_statement('RETURN "kuzu is cool" AS a;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0

    # check if position in history was maintained
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("is cool")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_finished_statement("\x1b[A\r")  # move up in history
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_k(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS aabc;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("As a")
    test.send_control_statement(KEY_ACTION.CTRL_K.value)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_d(temp_db) -> None:
    # ctrl d accepts search and acts as EOF when line is empty
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    # search gets accepted
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
    # acts as EOF
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1

    # test accepts search and delete
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases/ rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases/ rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_l(temp_db) -> None:
    # clear screen with empty line
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_statement("databases")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0

    # clear screen with non empty line
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
def test_cancel_search(temp_db, key) -> None:
    # check if line is cleared
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databases")
    test.send_control_statement(key)
    test.send_finished_statement('RETURN "kuzu is cool" AS a;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0

    # check if position in history was maintained
    test = ShellTest().add_argument(temp_db)
    test.start()
    set_up_search(test)
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("is cool")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_statement("\x1b[B")  # move down in history
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_H.value,
        KEY_ACTION.BACKSPACE.value,
        KEY_ACTION.CTRL_W.value,
    ],
)
def test_backspace(temp_db, key) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_R.value)
    test.send_statement("databasesabc")
    test.send_statement(key * 3)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
