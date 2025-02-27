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
            "    :mode [mode]     set output mode (default: box)",
            "    :stats [on|off]     toggle query stats on or off",  
            "    :multiline     set multiline mode (default)",
            "    :singleline     set singleline mode",
            "    :highlight [on|off]     toggle syntax highlighting on or off",
            "    :render_errors [on|off]     toggle error highlighting on or off",
            "    :render_completion [on|off]     toggle completion highlighting on or off",
            "",
            "    Note: you can change and see several system configurations, such as num-threads, ",
            "          timeout, and progress_bar using Cypher CALL statements.",
            "          e.g. CALL THREADS=5; or CALL current_setting('threads') return *;",
            "          See: \x1B]8;;https://docs.kuzudb.com/cypher/configuration\x1B\\https://docs.kuzudb.com/cypher/configuration\x1B]8;;\x1B\\",
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
    result.check_not_stdout(["\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502"])

    # test 1 row shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_rows 1")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples, 1 shown)")
    result.check_stdout(["\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502"])

    # test setting back to default
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_rows 0")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN id;')
    )
    result = test.run()
    result.check_stdout("(21 tuples, 20 shown)")
    result.check_stdout(["\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502", "\u2502  \u00B7    \u2502"])


def test_max_width(temp_db, csv_path) -> None:
    # test all columns shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 400")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    result.check_stdout("id")
    result.check_stdout("fname")
    result.check_stdout("Gender")
    result.check_stdout("ISStudent")
    result.check_stdout("isWorker")
    result.check_stdout("age")
    result.check_stdout("eyeSight")
    result.check_stdout("birthdate")
    result.check_stdout("registerTime")
    result.check_stdout("lastJobDuration")
    result.check_stdout("workedHours")
    result.check_stdout("usedNames")
    result.check_stdout("courseScoresPerTerm")
    result.check_not_stdout("\u2502 ... \u2502")
    result.check_stdout("(13 columns)")

    # test 2 columns shown
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":max_width 44")
        .statement(f'LOAD FROM "{csv_path}" (HEADER=true) RETURN *;')
    )
    result = test.run()
    result.check_stdout("id")
    result.check_not_stdout("fname")
    result.check_not_stdout("Gender")
    result.check_not_stdout("ISStudent")
    result.check_not_stdout("isWorker")
    result.check_not_stdout("\u2502 age    \u2502")
    result.check_not_stdout("eyeSight")
    result.check_not_stdout("birthdate")
    result.check_not_stdout("registerTime")
    result.check_not_stdout("lastJobDuration")
    result.check_not_stdout("workedHours")
    result.check_not_stdout("usedNames")
    result.check_stdout("\u2502 ... \u2502")
    result.check_stdout("courseScoresPerTerm")
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
    result.check_stdout("id")
    result.check_stdout("fname")
    result.check_stdout("Gender")
    result.check_not_stdout("ISStudent")
    result.check_not_stdout("isWorker")
    result.check_not_stdout("\u2502 age    \u2502")
    result.check_not_stdout("eyeSight")
    result.check_not_stdout("birthdate")
    result.check_not_stdout("registerTime")
    result.check_not_stdout("lastJobDuration")
    result.check_not_stdout("workedHours")
    result.check_not_stdout("usedNames")
    result.check_stdout("\u2502 ... \u2502")
    result.check_stdout("courseScoresPerTerm")
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
    result.check_stdout("\u2502 Table LANGUAGE_CODE has been created. \u2502")
    result.check_not_stdout("\u2502 ... \u2502")
    result.check_stdout("(1 column)")


def output_mode_verification(result) -> None:
    result.check_stdout(
        [
            "Available output modes:",
            "    box (default):    Tables using unicode box-drawing characters",
            "    column:    Output in columns",
            "    csv:    Comma-separated values",
            "    html:    HTML table",
            "    json:    Results in a JSON array",
            "    jsonlines:    Results in a NDJSON format",
            "    latex:    LaTeX tabular environment code",
            "    line:    One value per line",
            "    list:    Values delimited by \"|\"",
            "    markdown:    Markdown table",
            "    table:    Tables using ASCII characters",
            "    tsv:    Tab-separated values",
            "    trash:    No output",
        ],
    )


def test_set_mode(temp_db) -> None:
    # test default mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\u2502 a              \u2502 b            \u2502")
    result.check_stdout("\u2502 Databases Rule \u2502 kuzu is cool \u2502")

    # test column mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode column")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a                b")
    result.check_stdout("Databases Rule   kuzu is cool")

    # test csv mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode csv")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a,b")
    result.check_stdout("Databases Rule,kuzu is cool")

    # test csv escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode csv")
        .statement('RETURN "This is a \\"test\\", with commas, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with commas, ""quotes"", and\nnewlines."')

    # test box mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        # box is default so need to switch to another mode first
        .statement(":mode csv")
        .statement(":mode box")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\u2502 a              \u2502 b            \u2502")
    result.check_stdout("\u2502 Databases Rule \u2502 kuzu is cool \u2502")

    # test html mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode html")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("<table>")
    result.check_stdout("<tr>")
    result.check_stdout("<th>a</th><th>b</th>")
    result.check_stdout("</tr>")
    result.check_stdout("<tr>")
    result.check_stdout("<td>Databases Rule</td><td>kuzu is cool</td>")
    result.check_stdout("</tr>")
    result.check_stdout("</table>")

    # test html escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode html")
        .statement('RETURN "This is a <test> & \\"example\\" with \'special\' characters." AS a;')
    )
    result = test.run()
    result.check_stdout("<table>")
    result.check_stdout("<tr>")
    result.check_stdout("<th>a</th>")
    result.check_stdout("</tr>")
    result.check_stdout("<tr>")
    result.check_stdout("<td>This is a &lt;test&gt; &amp; &quot;example&quot; with &apos;special&apos; characters.</td>")
    result.check_stdout("</tr>")
    result.check_stdout("</table>")

    # test json mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode json")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout('[\n{"a":"Databases Rule","b":"kuzu is cool"}\n]')

    # test json escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode json")
        .statement('RETURN "This is a \\"test\\" with backslashes \\\\, newlines\n, and tabs \t." AS a;')
    )
    result = test.run()
    result.check_stdout('[\n{"a":"This is a \\"test\\" with backslashes \\\\, newlines\\n, and tabs \\t."}\n]')

    # test jsonlines mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode jsonlines")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout('{"a":"Databases Rule","b":"kuzu is cool"}')

    # test jsonlines escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode jsonlines")
        .statement('RETURN "This is a \\"test\\" with backslashes \\\\, newlines\n, and tabs \t." AS a;')
    )
    result = test.run()
    result.check_stdout('{"a":"This is a \\"test\\" with backslashes \\\\, newlines\\n, and tabs \\t."}')

    # test latex mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode latex")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("\\begin{tabular}{ll}")
    result.check_stdout("\\hline")
    result.check_stdout("a&b\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("Databases Rule&kuzu is cool\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("\\end{tabular}")

    # test latex escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode latex")
        .statement('RETURN "This is a test with special characters: %, $, &, #, _, {, }, ~, ^, \\\\, <, and >." AS a;')
    )
    result = test.run()
    result.check_stdout("\\begin{tabular}{l}")
    result.check_stdout("\\hline")
    result.check_stdout("a\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("This is a test with special characters: \\%, \\$, \\&, \\#, \\_, \\{, \\}, \\textasciitilde{}, \\textasciicircum{}, \\textbackslash{}, \\textless{}, and \\textgreater{}.\\\\")
    result.check_stdout("\\hline")
    result.check_stdout("\\end{tabular}")

    # test line mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode line")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a = Databases Rule")
    result.check_stdout("b = kuzu is cool")
    
    # test list mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode list")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a|b")
    result.check_stdout("Databases Rule|kuzu is cool")

    # test list escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode tsv")
        .statement('RETURN "This is a \\"test\\", with vertical bars |, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with vertical bars |, ""quotes"", and\nnewlines."')

    # test markdown mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode markdown")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("| a              | b            |")
    result.check_stdout("| Databases Rule | kuzu is cool |")

    # test table mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode table")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("| a              | b            |")
    result.check_stdout("| Databases Rule | kuzu is cool |")

    # test tsv mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode tsv")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("a\tb")
    result.check_stdout("Databases Rule\tkuzu is cool")

    # test tsv escaping
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode tsv")
        .statement('RETURN "This is a \\"test\\", with tabs \t, \\"quotes\\", and\nnewlines.";')
    )
    result = test.run()
    result.check_stdout('"This is a ""test"", with tabs \t, ""quotes"", and\nnewlines."')

    # test trash mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode trash")
        .statement('RETURN RANGE(0, 10) AS a;')
    )
    result = test.run()
    result.check_not_stdout("[0,1,2,3,4,5,6,7,8,9,10]")

    # test mode info
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode invalid")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    output_mode_verification(result)

    # test invalid mode
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":mode invalid")
        .statement('RETURN "Databases Rule" AS a, "kuzu is cool" AS b;')
    )
    result = test.run()
    result.check_stdout("Cannot parse 'invalid' as output mode.")
    output_mode_verification(result)


def test_stats(temp_db) -> None:
    # test stats default
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_stdout("(1 tuple)")
    result.check_stdout("(1 column)")
    result.check_stdout("Time: ")

    # test stats off
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":stats off")
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_not_stdout("(1 tuple)")
    result.check_not_stdout("(1 column)")
    result.check_not_stdout("Time: ")

    # test stats on
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":stats on")
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_stdout("(1 tuple)")
    result.check_stdout("(1 column)")
    result.check_stdout("Time: ")

    # test stats invalid
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":stats invalid")
        .statement('RETURN "Databases Rule" AS a;')
    )
    result = test.run()
    result.check_stdout("Cannot parse 'invalid' to toggle stats. Expect 'on' or 'off'.")


def test_multiline(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":multiline")
        .statement("RETURN")
        .statement('"databases rule"')
        .statement("AS")
        .statement("a")
        .statement(";")
    )
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")

    # test no truncation
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("a" * 400 + ";")
    )
    result = test.run()
    result.check_stdout("a" * 400)


def test_singleline(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":multiline")
        .statement(":singleline")
        .statement("RETURN")
        .statement('"databases rule"')
        .statement("AS")
        .statement("a")
        .statement(";")
    )
    result = test.run()
    result.check_stdout("\u2502 databases rule \u2502")

    # test truncation
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement("a" * 400 + ";")
    )
    result = test.run()
    result.check_stdout("a" * 80)


def test_highlight(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":highlight off")
    )
    result = test.run()
    result.check_stdout("disabled syntax highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":highlight on")
    )
    result = test.run()
    result.check_stdout("enabled syntax highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":highlight o")
    )
    result = test.run()
    result.check_stdout("Cannot parse 'o' to toggle highlighting. Expect 'on' or 'off'.")


def test_render_errors(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_errors off")
    )
    result = test.run()
    result.check_stdout("disabled error highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_errors on")
    )
    result = test.run()
    result.check_stdout("enabled error highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_errors o")
    )
    result = test.run()
    result.check_stdout("Cannot parse 'o' to toggle error highlighting. Expect 'on' or 'off'.")


def test_completion_highlighting(temp_db) -> None:
    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_completion off")
    )
    result = test.run()
    result.check_stdout("disabled completion highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_completion on")
    )
    result = test.run()
    result.check_stdout("enabled completion highlighting")

    test = (
        ShellTest()
        .add_argument(temp_db)
        .statement(":render_completion o")
    )
    result = test.run()
    result.check_stdout("Cannot parse 'o' to toggle completion highlighting. Expect 'on' or 'off'.")


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
