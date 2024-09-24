#!/usr/bin/env python3
from __future__ import annotations

import graphlib
import os
import sys
import shutil
import logging

from typing import Optional, Set
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

SCRIPT_DIR = Path(__file__).parent
HEADER_BASE_PATH = (SCRIPT_DIR / "../src/include").resolve()
MAIN_HEADER_PATH = HEADER_BASE_PATH / "main"
START_POINT = MAIN_HEADER_PATH / "kuzu.h"
JSON_HEADER_PATH = (SCRIPT_DIR / "../third_party/nlohmann_json/json_fwd.hpp").resolve()
ALP_HEADERS_PATH = (SCRIPT_DIR / "../third_party/alp/include").resolve()
OUTPUT_PATH = "kuzu.hpp"

logging.debug("HEADER_BASE_PATH: %s", HEADER_BASE_PATH)
logging.debug("MAIN_HEADER_PATH: %s", MAIN_HEADER_PATH)
logging.debug("START_POINT: %s", START_POINT)
logging.debug("JSON_HEADER_PATH: %s", JSON_HEADER_PATH)


def resolve_include(source_header: Path, include_path: str) -> Optional[Path]:
    if include_path == "json_fwd.hpp":
        return JSON_HEADER_PATH

    current_directory_include = source_header.parent / include_path
    if current_directory_include.exists():
        return current_directory_include

    main_directory_include = HEADER_BASE_PATH / include_path
    if main_directory_include.exists():
        return main_directory_include

    alp_directory_include = ALP_HEADERS_PATH / include_path
    if alp_directory_include.exists():
        return alp_directory_include

    return None


processed_headers: Set[Path] = set()
headers: Set[Path] = set()
with open(SCRIPT_DIR / "headers.txt") as file:
    for path in file:
        if not path.strip().startswith("#"):
            headers.add((SCRIPT_DIR / ".." / path.strip()).resolve())


def build_graph(graph: graphlib.TopologicalSorter, source_file: Path) -> None:
    assert source_file.is_absolute()
    global processed_headers
    if source_file in processed_headers:
        return
    processed_headers.add(source_file.resolve())

    with source_file.open("r") as f:
        for line in f.readlines():
            if not line.startswith('#include "'):
                continue
            header_path = line.split('"')[1]
            header_real_path = resolve_include(source_file, header_path)
            if header_real_path is None:
                logging.error(f"Could not find {header_path} included in {source_file}")
                sys.exit(1)
            logging.debug(
                f"Resolved {header_path} in {source_file} to {header_real_path}"
            )
            graph.add(source_file, header_real_path)
            build_graph(graph, header_real_path)


def create_merged_header():
    graph = graphlib.TopologicalSorter()
    logging.info("Building dependency graph...")
    build_graph(graph, START_POINT)
    if processed_headers != headers:
        if processed_headers - headers:
            error_string = os.linesep.join(
                sorted("\t" + str(header) for header in processed_headers - headers)
            )
            raise RuntimeError(
                "Extra headers were found in the include tree. "
                "Double-check that these headers should be included in the single file header "
                f"and if so, add them to headers.txt:{os.linesep}{error_string}"
            )
        if headers - processed_headers:
            error_string = os.linesep.join(
                sorted(str(header) for header in headers - processed_headers)
            )
            raise RuntimeError(
                "Missing headers from the include tree. "
                "Double-check that these headers are no longer needed in the single file header "
                f"and if so, remove them from headers.txt:{os.linesep}{error_string}"
            )
    logging.info("Topological sorting...")
    logging.info("Writing merged header...")
    with open(OUTPUT_PATH, "w") as f:
        f.write("#pragma once\n")
        for header_path in graph.static_order():
            with header_path.open() as f2:
                for line in f2.readlines():
                    if not (
                        line.startswith("#pragma once") or line.startswith('#include "')
                    ):
                        f.write(line)

    logging.info("Done!")


if __name__ == "__main__":
    create_merged_header()
