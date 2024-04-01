import os

import pexpect
import pytest
from conftest import ShellTest
from test_helper import KEY_ACTION, deleteIfExists


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
def test_sigint(temp_db, key) -> None:
    # test two consecutive signit required to exit shell
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(key)
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1

    # test sending line interupts the two signits to quit
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(key)
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1

    # test sigint cancels the query
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement("CREATE NODE TABLE t0(i STRING, PRIMARY KEY(i));")
    test.send_control_statement(key)
    test.send_finished_statement("MATCH (a:t0) RETURN *;\r")
    assert (
        test.shell_process.expect_exact(
            ["Error: Binder exception: Table t0 does not exist.", pexpect.EOF],
        )
        == 0
    )


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.ENTER.value,
        "\n",
    ],
)
def test_enter(temp_db, key, history_path) -> None:
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_finished_statement(key)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0

    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == 'RETURN "databases rule" AS a;\n'
    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.BACKSPACE.value,
        KEY_ACTION.CTRL_H.value,
    ],
)
def test_backspace(temp_db, key) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement(key * 3)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_d(temp_db) -> None:
    # ctrl d acts as EOF when line is empty
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1

    # ctrl d deletes characters to the right
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement("\x1b[D" * 3)  # move cursor to the left
    for _ in range(4):
        test.send_control_statement(KEY_ACTION.CTRL_D.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_t(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS ;a')
    test.send_statement("\x1b[D")  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_T.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_T.value)  # line does not refresh
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_b(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement(";")
    test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_B.value)  # line does not refresh
    test.send_finished_statement('RETURN "databases rule" AS a\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_f(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a')
    test.send_statement("\x1b[D")  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_F.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_F.value)  # line does not refresh
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_p(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_P.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_n(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_statement("\x1b[A" * 2)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_N.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_S.value,
    ],
)
def test_search(temp_db, key, history_path) -> None:
    # test search opens
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
    assert test.shell_process.expect_exact(["bck-i-search: _", pexpect.EOF]) == 0

    # test successful search
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_statement("databases")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0

    # test failing search
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["| kuzu is cool |", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_statement("databases*******************")
    assert test.shell_process.expect_exact(["failing-bck-i-search:", pexpect.EOF]) == 0
    # enter should process last match
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0
    deleteIfExists(os.path.join(history_path, "history.txt"))

    # test starting search with text inputted already
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_u(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement("CREATE NODE TABLE t0(i STRING, PRIMARY KEY(i));")
    test.send_control_statement(KEY_ACTION.CTRL_U.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    test.send_finished_statement("MATCH (a:t0) RETURN *;\r")
    assert (
        test.shell_process.expect_exact(
            ["Error: Binder exception: Table t0 does not exist.", pexpect.EOF],
        )
        == 0
    )


def test_ctrl_k(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement("\x1b[D" * 3)  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_K.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_a(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('ETURN "databases rule" AS a;')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)
    test.send_finished_statement("R\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_e(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a')
    test.send_statement("\x1b[H")  # move cursor to the front
    test.send_control_statement(KEY_ACTION.CTRL_E.value)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_l(temp_db) -> None:
    # clear screen with empty line
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0

    # clear screen with non empty line
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a')
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_ctrl_w(temp_db) -> None:
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement('RETURN "databases rule" AS a; abc def ghi ')
    test.send_statement(KEY_ACTION.CTRL_W.value * 3)
    test.send_statement("\x1b[H")  # move cursor to the front
    test.send_statement(KEY_ACTION.CTRL_W.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["| databases rule |", pexpect.EOF]) == 0


def test_tab(temp_db) -> None:
    # tab is interpreted as four psaces when pasting input
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_finished_statement('RETURN "databases\trule" AS a;\r')
    assert test.shell_process.expect_exact(["| databases    rule |", pexpect.EOF]) == 0

    # tab completion
    test = ShellTest().add_argument(temp_db)
    test.start()
    test.send_statement("CRE")
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(" N")
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(" TAB")
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_statement("LE t0(i STRING, PRIMARY KEY(i));")
    test.send_statement(KEY_ACTION.TAB.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert (
        test.shell_process.expect_exact(
            ["| Table t0 has been created. |", pexpect.EOF],
        )
        == 0
    )
