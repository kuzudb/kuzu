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
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_sigint_two_consecutive(key, test) -> None:
    # test two consecutive signit required to exit shell
    test.send_control_statement(key)
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_sigint_line_interupts(key, test) -> None:
    # test sending line interupts the two signits to quit
    test.send_control_statement(key)
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_C.value,
        KEY_ACTION.CTRL_G.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_sigint_cancels_query(key, test) -> None:
    # test sigint cancels the query
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
@pytest.mark.parametrize('mode', [':multiline\n', ':singleline\n'])
def test_enter(temp_db, key, history_path, mode) -> None:
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    test.send_finished_statement(mode)
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_finished_statement(key)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0

    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == mode
        assert f.readline() == 'RETURN "databases rule" AS a;\n'
    deleteIfExists(os.path.join(history_path, "history.txt"))

    # enter in multiline when not at the end of the line should not execute the query
    test = (
        ShellTest()
        .add_argument(temp_db)
        .add_argument("-p")
        .add_argument(history_path)
    )
    test.start()
    test.send_finished_statement(mode)
    test.send_statement('RETURN "databases rule" AS a;\x1b[D\x1b[D\x1b[D')
    test.send_finished_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0

    with open(os.path.join(history_path, "history.txt")) as f:
        assert f.readline() == mode
        assert f.readline() != 'RETURN "databases rule" AS a;\n'
    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.BACKSPACE.value,
        KEY_ACTION.CTRL_H.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_backspace(key, test) -> None:
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement(key * 3)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_d_eof(test) -> None:
    # ctrl d acts as EOF when line is empty
    test.send_control_statement(KEY_ACTION.CTRL_D.value)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 1


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_d_delete(test) -> None:
    # ctrl d deletes characters to the right
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement("\x1b[D" * 3)  # move cursor to the left
    for _ in range(4):
        test.send_control_statement(KEY_ACTION.CTRL_D.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_t(test) -> None:
    test.send_statement('RETURN "databases rule" AS ;a')
    test.send_statement("\x1b[D")  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_T.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_T.value)  # line does not refresh
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_b(test) -> None:
    test.send_statement(";")
    test.send_control_statement(KEY_ACTION.CTRL_B.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_B.value)  # line does not refresh
    test.send_finished_statement('RETURN "databases rule" AS a\x1b[C\r') # move cursor to the right one
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_f(test) -> None:
    test.send_statement('RETURN "databases rule" AS a')
    test.send_statement("\x1b[D")  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_F.value)
    test.shell_process.sendcontrol(KEY_ACTION.CTRL_F.value)  # line does not refresh
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_p(test) -> None:
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_control_statement(KEY_ACTION.CTRL_P.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_n(test) -> None:
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_statement("\x1b[A" * 2)  # move up in history
    test.send_control_statement(KEY_ACTION.CTRL_N.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_S.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search_opens(test, key, history_path) -> None:
    # test search opens
    test.send_control_statement(key)
    assert test.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
    assert test.shell_process.expect_exact(["bck-i-search: _", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_S.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search_good(test, key) -> None:
    # test successful search
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_statement("databases")
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_S.value,
    ],
)
@pytest.mark.parametrize('mode', [':multiline\n', ':singleline\n'])
def test_search_bad(mode, temp_db, key, history_path) -> None:
    # test failing search
    test = ShellTest().add_argument(temp_db).add_argument("-p").add_argument(history_path)
    test.start()
    test.send_finished_statement(mode)
    test.send_finished_statement('RETURN "databases rule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    test.send_finished_statement('RETURN "kuzu is cool" AS b;\r')
    assert test.shell_process.expect_exact(["\u2502 kuzu is cool \u2502", pexpect.EOF]) == 0
    test.send_control_statement(key)
    test.send_statement("databases are cool")
    assert test.shell_process.expect_exact(["failing-bck-i-search:", pexpect.EOF]) == 0
    # enter should process last match
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0
    deleteIfExists(os.path.join(history_path, "history.txt"))


@pytest.mark.parametrize(
    "key",
    [
        KEY_ACTION.CTRL_R.value,
        KEY_ACTION.CTRL_S.value,
    ],
)
@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_search(test, key) -> None:
    # test starting search with text inputted already
    test.send_statement('RETURN "databases rule" AS a;')
    test.send_control_statement(key)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_u(test) -> None:
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


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_k(test) -> None:
    test.send_statement('RETURN "databases rule" AS a;abc')
    test.send_statement("\x1b[D" * 3)  # move cursor to the left
    test.send_control_statement(KEY_ACTION.CTRL_K.value)
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_a(test) -> None:
    test.send_statement('ETURN "databases rule" AS a;')
    test.send_control_statement(KEY_ACTION.CTRL_A.value)
    test.send_finished_statement("R\x1b[F\r") # move cursor to end of line
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_e(test) -> None:
    test.send_statement('RETURN "databases rule" AS a')
    test.send_statement("\x1b[H")  # move cursor to the front
    test.send_control_statement(KEY_ACTION.CTRL_E.value)
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_l_empty(test) -> None:
    # clear screen with empty line
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_l_non_empty(test) -> None:
    # clear screen with non empty line
    test.send_statement('RETURN "databases rule" AS a')
    test.send_control_statement(KEY_ACTION.CTRL_L.value)
    assert test.shell_process.expect_exact(["\x1b[H\x1b[2J", pexpect.EOF]) == 0
    test.send_finished_statement(";\r")
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_ctrl_w(test) -> None:
    test.send_statement('RETURN "databases rule" AS a; abc def ghi ')
    test.send_statement(KEY_ACTION.CTRL_W.value * 3)
    test.send_statement("\x1b[H")  # move cursor to the front
    test.send_statement(KEY_ACTION.CTRL_W.value)
    test.send_statement("\x1b[F")  # move cursor to the end
    test.send_finished_statement(KEY_ACTION.ENTER.value)
    assert test.shell_process.expect_exact(["\u2502 databases rule \u2502", pexpect.EOF]) == 0


@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_tab_spaces(test) -> None:
    # tab is interpreted as four spaces when pasting input
    test.send_finished_statement('RETURN "databases\trule" AS a;\r')
    assert test.shell_process.expect_exact(["\u2502 databases    rule \u2502", pexpect.EOF]) == 0

@pytest.mark.parametrize('test', ['multiline', 'singleline'], indirect=True)
def test_tab(test) -> None:
    # tab completion
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
            ["\u2502 Table t0 has been created. \u2502", pexpect.EOF],
        )
        == 0
    )
