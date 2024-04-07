from __future__ import annotations

import os
import shutil
import subprocess

import pexpect
import pytest
from test_helper import KUZU_EXEC_PATH, KUZU_ROOT, deleteIfExists


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--start-offset",
        action="store",
        type=int,
        help="Skip the first 'n' tests",
    )


def pytest_collection_modifyitems(config, items) -> None:
    start_offset = config.getoption("--start-offset")
    if not start_offset:
        # --skiplist not given in cli, therefore move on
        return

    skipped = pytest.mark.skip(reason="included in --skiplist")
    skipped_items = items[:start_offset]
    for item in skipped_items:
        item.add_marker(skipped)


class TestResult:
    def __init__(self, stdout, stderr, status_code) -> None:
        self.stdout: str | bytes = stdout
        self.stderr: str | bytes = stderr
        self.status_code: int = status_code

    def check_stdout(self, expected: str | list[str] | bytes) -> None:
        if isinstance(expected, list):
            expected = "\n".join(expected)
        assert self.status_code == 0
        assert expected in self.stdout

    def check_not_stdout(self, expected: str | list[str] | bytes) -> None:
        if isinstance(expected, list):
            expected = "\n".join(expected)
        assert self.status_code == 0
        assert expected not in self.stdout

    def check_stderr(self, expected: str) -> None:
        assert expected in self.stderr


class ShellTest:
    def __init__(self) -> None:
        self.shell = KUZU_EXEC_PATH
        self.arguments = [self.shell]
        self.statements: list[str] = []
        self.input = None
        self.output = None
        self.environment = {}
        self.shell_process = None

    def add_argument(self, *args):
        self.arguments.extend(args)
        return self

    def statement(self, stmt):
        self.statements.append(stmt)
        return self

    def query(self, *stmts):
        self.statements.extend(stmts)
        return self

    def input_file(self, file_path):
        self.input = file_path
        return self

    def output_file(self, file_path):
        self.output = file_path
        return self

    # Test Running methods

    def get_command(self, cmd: str) -> list[str]:
        command = self.arguments
        if self.input:
            command += [cmd]
        return command

    def get_input_data(self, cmd: str):
        if self.input:
            with open(self.input, "rb") as f:
                input_data = f.read()
        else:
            input_data = bytearray(cmd, "utf8")
        return input_data

    def get_output_pipe(self):
        output_pipe = subprocess.PIPE
        if self.output:
            output_pipe = open(self.output, "w+")
        return output_pipe

    def get_statements(self):
        statements = []
        for statement in self.statements:
            statements.append(statement)
        return "\n".join(statements)

    def get_output_data(self, res):
        if self.output:
            stdout = open(self.output).read()
        else:
            stdout = res.stdout.decode("utf8").strip()
        stderr = res.stderr.decode("utf8").strip()
        return stdout, stderr

    def run(self):
        statements = self.get_statements()
        command = self.get_command(statements)
        input_data = self.get_input_data(statements)
        output_pipe = self.get_output_pipe()

        my_env = os.environ.copy()
        for key, val in self.environment.items():
            my_env[key] = val

        res = subprocess.run(
            command,
            input=input_data,
            stdout=output_pipe,
            stderr=subprocess.PIPE,
            env=my_env,
            check=False,
        )

        stdout, stderr = self.get_output_data(res)
        return TestResult(stdout, stderr, res.returncode)

    def start(self) -> None:
        command = " ".join(self.arguments)

        my_env = os.environ.copy()
        for key, val in self.environment.items():
            my_env[key] = val

        self.shell_process = pexpect.spawn(command, encoding="utf_8", env=my_env)

    def send_finished_statement(self, stmt: str) -> None:
        if self.shell_process:
            assert self.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
            self.shell_process.send(stmt)
            assert self.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0

    def send_statement(self, stmt: str) -> None:
        if self.shell_process:
            assert self.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
            self.shell_process.send(stmt)

    def send_control_statement(self, stmt: str) -> None:
        if self.shell_process:
            assert self.shell_process.expect_exact(["kuzu", pexpect.EOF]) == 0
            self.shell_process.sendcontrol(stmt)


@pytest.fixture()
def temp_db(tmp_path):
    shutil.rmtree(tmp_path, ignore_errors=True)
    return str(tmp_path)


@pytest.fixture()
def get_tmp_path(tmp_path):
    return str(tmp_path)


@pytest.fixture()
def history_path():
    path = os.path.join(KUZU_ROOT, "tools", "shell", "test", "files")
    deleteIfExists(os.path.join(path, "history.txt"))
    return path


@pytest.fixture()
def csv_path():
    return os.path.join(KUZU_ROOT, "tools", "shell", "test", "files", "vPerson.csv")
