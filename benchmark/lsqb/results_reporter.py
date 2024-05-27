import subprocess
import os
import datetime
import requests
from pathlib import Path
import logging
import sys
import json


REPORT_CSV_PATH = "/tmp/benchmark_result/results.csv"
LOG_PATH = "lsqb.log"
QUERIES_PATH = (Path(__file__).parent / "queries").resolve()


def _get_git_revision_hash():
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode("utf-8")
            .strip()
        )
    except:
        return None


def _get_commit_message():
    try:
        return (
            subprocess.check_output(["git", "log", "-1", "--pretty=%B"])
            .decode("utf-8")
            .strip()
        )
    except:
        return None


def _get_commit_author():
    try:
        return (
            subprocess.check_output(["git", "log", "-1", "--pretty=%an"])
            .decode("utf-8")
            .strip()
        )
    except:
        return None


def _get_commit_email():
    try:
        return (
            subprocess.check_output(["git", "log", "-1", "--pretty=%ae"])
            .decode("utf-8")
            .strip()
        )
    except:
        return None


def get_run_info():
    commit = {
        "hash": os.environ.get("GITHUB_SHA", _get_git_revision_hash()),
        "author": _get_commit_author(),
        "email": _get_commit_email(),
        "message": _get_commit_message(),
    }
    return {
        "commit": commit,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scale_factor": os.environ.get("SCALE_FACTOR", None),
    }


def get_log():
    try:
        with open(LOG_PATH, "r") as f:
            return f.read()
    except:
        return None


def get_queries():
    queries = {}
    for query_file in os.listdir(QUERIES_PATH):
        try:
            query_number = int(query_file.split("q")[1].split(".")[0])
            with open(os.path.join(QUERIES_PATH, query_file), "r") as f:
                query = f.read()
                queries[query_number] = query
        except:
            pass
    return queries


def get_results():
    queries = get_queries()
    with open(REPORT_CSV_PATH, "r") as f:
        lines = f.readlines()
        lines = [line.strip().split("\t") for line in lines]
        results = []
        for l in lines:
            try:
                query_number = int(l[1])
            except:
                query_number = None
            try:
                threads = int(l[2].split(" ")[0])
            except:
                threads = None
            try:
                is_query_timeout = l[4].strip().lower() == "timeout"
            except:
                is_query_timeout = None
            try:
                query_time = float(l[4].strip())
                is_query_success = True
            except:
                query_time = None
                is_query_success = False
            try:
                memory_usage = float(l[5].strip().split(" ")[0])
            except:
                memory_usage = None
            try:
                output = l[6]
            except:
                output = None
            results.append(
                {
                    "query_number": query_number,
                    "threads": threads,
                    "is_timeout": is_query_timeout,
                    "is_successful": is_query_success,
                    "query_time": query_time,
                    "memory_usage": memory_usage,
                    "output": output,
                    "query_statement": queries.get(query_number, None),
                }
            )
        return results


def get_payload():
    payload = get_run_info()
    payload["log"] = get_log()
    payload["benchmarks"] = get_results()
    payload["is_successful"] = all([b["is_successful"] for b in payload["benchmarks"]])
    return payload


def upload_benchmark_result(benchmark_server_url, jwt_token, payload):
    response = requests.post(
        benchmark_server_url,
        json=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + jwt_token,
        },
    )
    if response.status_code != 200:
        logging.error("An error has occurred while uploading benchmark result!")
        sys.exit(1)


def main():
    logging.basicConfig(level=logging.INFO)

    benchmark_server_url = os.getenv("LSQB_BENCHMARK_SERVER_URL")
    if benchmark_server_url is None:
        logging.error("LSQB_BENCHMARK_SERVER_URL is not set, exiting...")
        sys.exit(1)

    jwt_token = os.getenv("JWT_TOKEN")
    if jwt_token is None:
        logging.error("JWT_TOKEN is not set, exiting...")
        sys.exit(1)

    logging.info("Generating benchmark result payload...")
    payload = get_payload()
    logging.debug("Payload: %s", json.dumps(payload, indent=2))
    logging.info("Uploading benchmark result...")
    upload_benchmark_result(benchmark_server_url, jwt_token, payload)
    logging.info("Benchmark result uploaded successfully!")


if __name__ == "__main__":
    main()
