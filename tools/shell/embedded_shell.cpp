#include "tools/shell/include/embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <regex>
#include <sstream>

#include "src/common/include/type_utils.h"

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
    const string SYSTEM_DEBUG_INFO = ":system_debug_info";
    const string BUFFER_MANAGER_DEBUG_INFO = ":bm_debug_info";
    const vector<string> commandList = {HELP, CLEAR, QUIT, THREAD, BUFFER_MANAGER_SIZE,
        SYSTEM_DEBUG_INFO, BUFFER_MANAGER_DEBUG_INFO};
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

const string keywordColorPrefix = "\033[32m\033[1m";
const string keywordResetPostfix = "\033[00m";

const regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

void completion(const char* buffer, linenoiseCompletions* lc) {
    string buf = string(buffer);
    if (buf[0] == ':') {
        for (auto& command : shellCommand.commandList) {
            if (regex_search(command, regex("^" + buf))) {
                linenoiseAddCompletion(lc, command.c_str());
            }
        }
        return;
    }
    string prefix;
    auto lastKeywordPos = buf.rfind(' ') + 1;
    if (lastKeywordPos != string::npos) {
        prefix = buf.substr(0, lastKeywordPos);
        buf = buf.substr(lastKeywordPos);
    }
    if (buf.empty()) {
        return;
    }
    for (string keyword : keywordList) {
        string bufEscaped = regex_replace(buf, specialChars, R"(\$&)");
        if (regex_search(keyword, regex("^" + bufEscaped, regex_constants::icase))) {
            if (islower(buf[0])) {
                transform(keyword.begin(), keyword.end(), keyword.begin(), ::tolower);
            }
            linenoiseAddCompletion(lc, (prefix + keyword).c_str());
        }
    }
}

void highlight(char* buffer, char* resultBuf, uint32_t maxLen, uint32_t cursorPos) {
    string buf(buffer);
    ostringstream highlightBuffer;
    string word;
    vector<string> tokenList;
    if (cursorPos > maxLen) {
        buf = buf.substr(cursorPos - maxLen, maxLen);
    } else if (buf.length() > maxLen) {
        buf = buf.substr(0, maxLen);
    }
    for (auto i = 0u; i < buf.length(); i++) {
        if ((buf[i] != ' ' && !word.empty() && word[0] == ' ') ||
            (buf[i] == ' ' && !word.empty() && word[0] != ' ')) {
            tokenList.emplace_back(word);
            word = "";
        }
        word += buf[i];
    }
    tokenList.emplace_back(word);
    for (string& token : tokenList) {
        if (token.find(' ') == std::string::npos) {
            for (string keyword : keywordList) {
                if (regex_search(token, regex("^" + keyword + "$", regex_constants::icase)) ||
                    regex_search(token, regex("^" + keyword + "\\(", regex_constants::icase))) {
                    token =
                        regex_replace(token, regex("^(" + keyword + ")", regex_constants::icase),
                            keywordColorPrefix + "$1" + keywordResetPostfix);
                    break;
                }
            }
        }
        highlightBuffer << token;
    }
    strcpy(resultBuf, highlightBuffer.str().c_str());
}

EmbeddedShell::EmbeddedShell(
    const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig) {
    linenoiseHistoryLoad(HISTORY_PATH);
    linenoiseSetCompletionCallback(completion);
    linenoiseSetHighlightCallback(highlight);
    database = make_unique<Database>(databaseConfig, systemConfig);
    conn = make_unique<Connection>(database.get());
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
                auto numThreads = stoi(lineStr.substr(shellCommand.THREAD.length()));
                conn->setMaxNumThreadForExec(numThreads);
                printf("numThreads set as %d\n", numThreads);
            } catch (exception& e) { printf("%s\n", e.what()); }

        } else if (lineStr.rfind(shellCommand.BUFFER_MANAGER_SIZE) == 0) {
            try {
                auto newPageSize =
                    stoull(lineStr.substr(shellCommand.BUFFER_MANAGER_SIZE.length()));
                database->resizeBufferManager(newPageSize);
            } catch (exception& e) { printf("%s\n", e.what()); }

        } else if (lineStr.rfind(shellCommand.BUFFER_MANAGER_DEBUG_INFO) == 0) {
            printf("Buffer Manager Debug Info: \n %s \n",
                database->getBufferManager()->debugInfo()->dump(4).c_str());
        } else {
            auto queryResult = conn->query(lineStr);
            if (queryResult->isSuccess()) {
                printExecutionResult(*queryResult);
            } else {
                printf("Error: %s\n", queryResult->getErrorMessage().c_str());
            }
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
    printf("%s:bm_debug_info %sdebug information about the buffer manager\n", TAB, TAB);
    printf("%s:system_debug_info %sdebug information about the system\n", TAB, TAB);
}

void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->getIsExplain()) {
        querySummary->printPlanToStdOut();
    } else {
        auto numTuples = queryResult.getNumTuples();
        // print query result (numFlatTuples & tuples)
        printf(">> Number of output tuples: %lu\n", numTuples);
        printf(">> Compiling time: %.2fms\n", querySummary->getCompilingTime());
        printf(">> Executing time: %.2fms\n", querySummary->getExecutionTime());
        if (numTuples > 0) {
            vector<uint32_t> colsWidth(queryResult.getNumColumns(), 2);
            uint32_t lineSeparatorLen = 1u + colsWidth.size();
            string lineSeparator;
            while (queryResult.hasNext()) {
                auto tuple = queryResult.getNext();
                for (auto i = 0u; i < colsWidth.size(); i++) {
                    if (tuple->nullMask[i]) {
                        continue;
                    }
                    uint32_t fieldLen = TypeUtils::toString(tuple->getValue(i)).length() + 2;
                    colsWidth[i] = (fieldLen > colsWidth[i]) ? fieldLen : colsWidth[i];
                }
            }

            for (auto width : colsWidth) {
                lineSeparatorLen += width;
            }
            lineSeparator = string(lineSeparatorLen, '-');
            printf("%s\n", lineSeparator.c_str());

            queryResult.resetIterator();
            while (queryResult.hasNext()) {
                auto tuple = queryResult.getNext();
                printf("|%s|\n", tuple->toString(colsWidth, "|").c_str());
                printf("%s\n", lineSeparator.c_str());
            }
        }

        if (querySummary->getIsProfile()) {
            // print plan with profiling metrics
            printf("==============================================\n");
            printf("=============== Profiler Summary =============\n");
            printf("==============================================\n");
            printf(">> plan\n");
            querySummary->printPlanToStdOut();
        }
    }
}
