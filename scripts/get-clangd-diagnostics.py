#!/usr/bin/env python3
import argparse
import io
import json
import multiprocessing
import os
import subprocess
import sys


class LSPClient:
    def __init__(self, *, compile_commands_dir=None, jobs=None, verbose=False):
        args = ["clangd", "-j", str(jobs)]
        if jobs is None:
            jobs = multiprocessing.cpu_count()

        if compile_commands_dir is not None:
            args += ["--compile-commands-dir", compile_commands_dir]

        self.id = 0
        self.child = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # Let clangd inherit our stderr, or suppress it entirely.
            stderr=None if verbose else subprocess.DEVNULL,
        )
        self.stdin = io.TextIOWrapper(self.child.stdin, newline="\r\n")
        self.stdout = io.TextIOWrapper(self.child.stdout, newline="\r\n")

    def request(self, method, params):
        self.send_request(method, params)
        return self.recv_response()

    def send_request(self, method, params):
        self.id += 1
        self.send_json(
            dict(
                id=self.id,
                jsonrpc="2.0",
                method=method,
                params=params,
            )
        )

    def send_json(self, json_data):
        data = json.dumps(json_data)
        bindata = data.encode("utf-8")
        header = f"Content-Length: {len(bindata)}\r\n\r\n"

        self.stdin.write(header + data)
        self.stdin.flush()

    def recv_response(self):
        json_data = self.recv_json()
        assert json_data["id"] == self.id
        assert "error" not in json_data
        return json_data["result"]

    def send_notif(self, method, params):
        self.send_json(
            dict(
                jsonrpc="2.0",
                method=method,
                params=params,
            )
        )

    def expect_notif(self, method):
        json_data = self.recv_json()
        assert json_data["method"] == method
        assert "error" not in json_data
        return json_data["params"]

    def recv_json(self):
        header = self.stdout.readline()
        content_len_header = "Content-Length: "
        assert header.startswith(content_len_header)
        assert header.endswith("\r\n")
        data_len = int(header[len(content_len_header) : -2])

        # Expect end of header
        assert self.stdout.read(2) == "\r\n"

        data = self.stdout.read(data_len)
        return json.loads(data)

    def initialize(self, project):
        return self.request(
            "initialize",
            dict(
                processId=os.getpid(),
                rootUri="file://" + project,
                capabilities={},
            ),
        )

    def open_file(self, file):
        with open(file) as f:
            file_content = f.read()

        self.send_notif(
            "textDocument/didOpen",
            dict(
                textDocument=dict(
                    uri="file://" + os.path.realpath(file),
                    languageId="cpp",
                    version=1,
                    text=file_content,
                )
            ),
        )

    def show_diagnostics(self):
        diagnostics_response = self.expect_notif("textDocument/publishDiagnostics")
        uri = diagnostics_response["uri"]
        file_prefix = "file://"
        assert uri.startswith(file_prefix)
        file = uri[len(file_prefix) :]
        diagnostics = diagnostics_response["diagnostics"]
        for diagnostic in diagnostics:
            LSPClient.show_diagnostic(file, diagnostic)

        return len(diagnostics) != 0

    def show_diagnostic(file, diagnostic):
        range = diagnostic["range"]
        start = range["start"]
        line = start["line"]
        message = diagnostic["message"]
        print(f"{file}:{line}:{message}")

    def send_shutdown(self):
        self.send_request("shutdown", None)

    def recv_shutdown_send_exit(self):
        self.send_notif("exit", None)

    def wait(self):
        assert self.child.wait() == 0


def shutdown_all(clients):
    # Shutdown the clients in parallel. This drastically speeds up cleanup time.
    for client in clients:
        client.send_shutdown()

    for client in clients:
        client.recv_shutdown_send_exit()

    for client in clients:
        client.wait()


def get_clients(client_count, compile_commands_dir, total_jobs, verbose):
    # Distribute jobs evenly.
    # Clients near the front get more jobs, but they also may get more files.
    job_count = [0] * client_count
    for i in range(total_jobs):
        job_count[i % client_count] += 1
    return [LSPClient(compile_commands_dir=compile_commands_dir, jobs=jobs, verbose=verbose) for jobs in job_count]


def get_diagnostics(files, *, client_count, compile_commands_dir, total_jobs, verbose):
    if client_count > total_jobs:
        print(f"Client count {client_count} is greater than total jobs {total_jobs}. Forcing the client count to {total_jobs}.", file=sys.stderr)
        client_count = total_jobs

    project = os.getcwd()

    client_count = min(client_count, len(files))
    clients = get_clients(client_count, compile_commands_dir, total_jobs, verbose)
    for client in clients:
        client.initialize(project)

    # Similar to distributing jobs, we distribute files evenly.
    file_counts = [0] * client_count
    for i, file in enumerate(files):
        client_idx = i % client_count
        clients[client_idx].open_file(file)
        file_counts[client_idx] += 1

    any_diagnostic = 0
    for file_count, client in zip(file_counts, clients):
        for _ in range(file_count):
            any_diagnostic |= client.show_diagnostics()

    shutdown_all(clients)
    return any_diagnostic


def main():
    parser = argparse.ArgumentParser(
        prog="get-clangd-diagnostics.py",
        description="Scan project for any clangd diagnostics (including warnings) and outputs them.",
    )
    parser.add_argument("files", nargs="+", help="Files to scan")
    parser.add_argument(
        "--instances",
        type=int,
        default=4,
        help="Number of clangd instances to spawn in parallel. Defaults to 4.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help="Number of total jobs across all servers. Defaults to the CPU count.",
    )
    parser.add_argument(
        "-p",
        "--compile-commands-dir",
        help="Directory containining compile_commands.json",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show clangd debug output",
    )

    args = parser.parse_args(sys.argv[1:])
    jobs = args.jobs if args.jobs is not None else multiprocessing.cpu_count()
    return get_diagnostics(
        args.files,
        client_count=args.instances,
        compile_commands_dir=args.compile_commands_dir,
        total_jobs=jobs,
        verbose=args.verbose,
    )


sys.exit(main())
