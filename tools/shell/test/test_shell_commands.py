from conftest import ShellTest


def test_help(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement(":help")
    result = test.run()
    result.check_stdout(
        [
            "    :help     get command list",
            "    :clear     clear shell",
            "    :quit     exit from shell",
            "    :max_rows [max_rows]     set maximum number of rows for display (default: 20)",
            "    :max_width [max_width]     set maximum width in characters for display",
            "",
            "    Note: you can change and see several system configurations, such as num-threads, ",
            "          timeout, and progress_bar using Cypher CALL statements.",
            "          e.g. CALL THREADS=5; or CALL current_setting('threads') return *;",
            "          See: https://docs.kuzudb.com/cypher/configuration",
        ],
    )


def test_clear(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement(":clear")
    result = test.run()
    result.check_stdout("\x1b[H\x1b[2J")


def test_quit(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement(":quit").statement("RETURN RANGE(0,10) AS a;")
    result = test.run()
    # check to make sure the return query did not execute
    result.check_not_stdout("[0,1,2,3,4,5,6,7,8,9,10]")


def test_max_rows(temp_db, csv_path) -> None:
    # test all rows shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_rows 40")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples)")
    result.check_not_stdout(["|  . |", "|  . |", "|  . |"])

    # test 1 row shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_rows 1")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples, 1 shown)")
    result.check_stdout(["|  . |", "|  . |", "|  . |"])

    # test setting back to default
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_rows 0")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples, 20 shown)")
    result.check_stdout(["|  . |", "|  . |", "|  . |"])


def test_max_width(temp_db, csv_path) -> None:
    # test all columns shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 400")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    result.check_stdout("-" * 266)
    result.check_not_stdout("-" * 267)
    result.check_not_stdout("| ... |")
    result.check_stdout("(13 columns)")

    # test 2 columns shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 44")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    # while max width is 44, the columns that fit have a total width of 34
    result.check_stdout("-" * 34)
    result.check_not_stdout("-" * 35)
    result.check_stdout("| ... |")
    result.check_stdout("(13 columns, 2 shown)")

    # test too small to display (back to terminal width)
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 2")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    # terminal width when running test is 80
    result.check_stdout("-" * 80)
    result.check_not_stdout("-" * 81)
    result.check_stdout("| ... |")
    result.check_stdout("(13 columns, 4 shown)")

    # test result tuples unaffected by width
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 2")
        .statement(
            "CREATE NODE TABLE LANGUAGE_CODE(alpha2 STRING, English STRING, PRIMARY KEY (alpha2));",
        )
    )
    result = test.run()
    # terminal width when running test is 80
    result.check_stdout("Table LANGUAGE_CODE has been created.")
    result.check_not_stdout("| ... |")
    result.check_stdout("(1 column)")


def test_bad_command(temp_db) -> None:
    test = ShellTest().add_argument(temp_db).statement(":maxrows").statement(":quiy").statement("clearr;")
    result = test.run()
    result.check_stdout(
        'Error: Unknown command: ":maxrows". Enter ":help" for help',
    )
    result.check_stdout('Did you mean: ":max_rows"?')
    result.check_stdout('Error: Unknown command: ":quiy". Enter ":help" for help')
    result.check_stdout('Did you mean: ":quit"?')
    result.check_stdout(
        '"clearr;" is not a valid Cypher query. Did you mean to issue a CLI command, e.g., ":clear"?',
    )
