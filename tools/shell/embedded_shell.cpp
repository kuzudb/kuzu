#include "tools/shell/include/embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <regex>

#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

// prompt for user input
const char* PROMPT = "graphflowdb> ";
// file to read/write shell history
const char* HISTORY_PATH = "history.txt";

// build-in shell command
struct ShellCommand {
    const string HELP = ":help";
    const string CLEAR = ":clear";
    const string QUIT = ":quit";
    const string THREAD = ":thread";
    const string BUFFER_MANAGER_SIZE = ":buffer_manager_size";
    const vector<string> commandList = {HELP, CLEAR, QUIT, THREAD, BUFFER_MANAGER_SIZE};
} shellCommand;

const char* TAB = "    ";

const vector<string> keywordList = {"CALL", "CREATE", "DELETE", "DETACH", "EXISTS", "FOREACH",
    "LOAD", "MATCH", "MERGE", "OPTIONAL", "REMOVE", "RETURN", "SET", "START", "UNION", "UNWIND",
    "WITH", "LIMIT", "ORDER", "SKIP", "WHERE", "YIELD", "ASC", "ASCENDING", "ASSERT", "BY", "CSV",
    "DESC", "DESCENDING", "ON", "ALL", "CASE", "ELSE", "END", "THEN", "WHEN", "AND", "AS",
    "CONTAINS", "DISTINCT", "ENDS", "IN", "IS", "NOT", "OR", "STARTS", "XOR", "CONSTRAINT",
    "CREATE", "DROP", "EXISTS", "INDEX", "NODE", "KEY", "UNIQUE", "INDEX", "JOIN", "PERIODIC",
    "COMMIT", "SCAN", "USING", "FALSE", "NULL", "TRUE", "ADD", "DO", "FOR", "MANDATORY", "OF",
    "REQUIRE", "SCALAR", "EXPLAIN", "PROFILE", "HEADERS", "FROM", "FIELDTERMINATOR", "STAR",
    "MINUS", "COUNT"};

void completion(const char* buffer, linenoiseCompletions* lc) {
    string buf = string(buffer);
    if (buf[0] == ':') {
        for (string shellCommand : shellCommand.commandList) {
            if (regex_search(shellCommand, regex("^" + buf))) {
                linenoiseAddCompletion(lc, shellCommand.c_str());
            }
        }
        return;
    }
    string prefix("");
    auto lastKeywordPos = buf.rfind(' ') + 1;
    if (lastKeywordPos != string::npos) {
        prefix = buf.substr(0, lastKeywordPos);
        buf = buf.substr(lastKeywordPos);
    }
    if (buf == "") {
        return;
    }
    for (string keyword : keywordList) {
        if (regex_search(keyword, regex("^" + buf, regex_constants::icase))) {
            if (islower(buf[0])) {
                transform(keyword.begin(), keyword.end(), keyword.begin(), ::tolower);
            }
            linenoiseAddCompletion(lc, (prefix + keyword).c_str());
        }
    }
}

void EmbeddedShell::initialize() {
    linenoiseHistoryLoad(HISTORY_PATH);
    linenoiseSetCompletionCallback(completion);
}

void EmbeddedShell::run() {
    char* line;
    while ((line = linenoise(PROMPT)) != nullptr) {
        auto lineStr = string(line);
        if (line == shellCommand.HELP) {
            printHelp();
        } else if (line == shellCommand.CLEAR) {
            linenoiseClearScreen();
        } else if (line == shellCommand.QUIT) {
            free(line);
            break;
        } else if (lineStr.rfind(shellCommand.THREAD) == 0) {
            try {
                context.numThreads = stoi(lineStr.substr(shellCommand.THREAD.length()));
                printf("numThreads set as %lu\n", context.numThreads);
            } catch (exception& e) { printf("%s\n", e.what()); }

        } else if (lineStr.rfind(shellCommand.BUFFER_MANAGER_SIZE) == 0) {
            try {
                system.bufferManager->resize(
                    stoull(lineStr.substr(shellCommand.BUFFER_MANAGER_SIZE.length())));
            } catch (exception& e) { printf("%s\n", e.what()); }

        } else {
            context.query = lineStr;
            try {
                system.executeQuery(context);
                printExecutionResult();
            } catch (exception& e) { printf("%s\n", e.what()); }
        }
        linenoiseHistoryAdd(line);
        linenoiseHistorySave("history.txt");
        free(line);
    }
}

void EmbeddedShell::printHelp() {
    printf("%s:help %sget command list\n", TAB, TAB);
    printf("%s:clear %sclear shell\n", TAB, TAB);
    printf("%s:quit %sexit from shell\n", TAB, TAB);
    printf("%s:thread [thread] %snumber of threads for execution\n", TAB, TAB);
}

void EmbeddedShell::printExecutionResult() {
    if (context.enable_explain) {
        // print plan without execution
        string plan = context.planPrinter->printPlanToJson(*context.profiler).dump(4);
        printf("%s\n", plan.c_str());
    } else {
        // print query result (numTuples & tuples)
        printf(">> Number of output tuples: %lu\n", context.queryResult->numTuples);
        printf(">> Compiling time: %.2fms\n", context.compilingTime);
        printf(">> Executing time: %.2fms\n", context.executingTime);
        if (!context.queryResult->resultSetCollection.empty()) {
            ResultSetIterator resultSetIterator(context.queryResult->resultSetCollection[0].get());
            Tuple tuple(resultSetIterator.dataTypes);
            vector<uint32_t> colsWidth(tuple.len(), 2);
            uint32_t lineSeparatorLen = 1u + colsWidth.size();
            string lineSeparator;
            //  first loop: calculate column width of the table
            for (auto& resultSet : context.queryResult->resultSetCollection) {
                resultSetIterator.setResultSet(resultSet.get());
                while (resultSetIterator.hasNextTuple()) {
                    resultSetIterator.getNextTuple(tuple);
                    for (auto i = 0u; i < colsWidth.size(); i++) {
                        if (tuple.nullMask[i]) {
                            continue;
                        }
                        uint32_t fieldLen = tuple.getValue(i)->toString().length() + 2;
                        colsWidth[i] = (fieldLen > colsWidth[i]) ? fieldLen : colsWidth[i];
                    }
                }
            }
            for (auto width : colsWidth) {
                lineSeparatorLen += width;
            }
            lineSeparator = string(lineSeparatorLen, '-');
            printf("%s\n", lineSeparator.c_str());
            for (auto& resultSet : context.queryResult->resultSetCollection) {
                resultSetIterator.setResultSet(resultSet.get());
                while (resultSetIterator.hasNextTuple()) {
                    resultSetIterator.getNextTuple(tuple);
                    for (uint32_t k = 0; k < resultSet->multiplicity; k++) {
                        printf("|%s|\n", tuple.toString(colsWidth, "|").c_str());
                        printf("%s\n", lineSeparator.c_str());
                    }
                }
            }
        }
        if (context.profiler->enabled) {
            // print plan with profiling metrics
            printf("==============================================\n");
            printf("=============== Profiler Summary =============\n");
            printf("==============================================\n");
            printf(">> Compiling time: %.2fms\n", context.compilingTime);
            printf(">> Executing time: %.2fms\n", context.executingTime);
            printf(">> plan\n");
            string plan = context.planPrinter->printPlanToJson(*context.profiler).dump(4);
            printf("%s\n", plan.c_str());
        }
    }
}
