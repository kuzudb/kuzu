#include "tools/shell/include/embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <regex>

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
    const string BUFFER_MANAGER_DEBUG_INFO = ":bm_debug_info";
    const string BUILT_IN_FUNCTIONS = ":functions";
    const string LIST_NODES = ":list_nodes";
    const string LIST_RELS = ":list_rels";
    const string SHOW_NODE = ":show_node";
    const string SHOW_REL = ":show_rel";
    const vector<string> commandList = {HELP, CLEAR, QUIT, THREAD, BUFFER_MANAGER_SIZE,
        BUFFER_MANAGER_DEBUG_INFO, BUILT_IN_FUNCTIONS, LIST_NODES, LIST_RELS, SHOW_NODE, SHOW_REL};
} shellCommand;

const char* TAB = "    ";

const vector<string> keywordList = {"CALL", "CREATE", "DELETE", "DETACH", "EXISTS", "FOREACH",
    "LOAD", "MATCH", "MERGE", "OPTIONAL", "REMOVE", "RETURN", "SET", "START", "UNION", "UNWIND",
    "WITH", "LIMIT", "ORDER", "SKIP", "WHERE", "YIELD", "ASC", "ASCENDING", "ASSERT", "BY", "CSV",
    "DESC", "DESCENDING", "ON", "ALL", "CASE", "ELSE", "END", "THEN", "WHEN", "AND", "AS", "REL",
    "TABLE", "CONTAINS", "DISTINCT", "ENDS", "IN", "IS", "NOT", "OR", "STARTS", "XOR", "CONSTRAINT",
    "DROP", "EXISTS", "INDEX", "NODE", "KEY", "UNIQUE", "INDEX", "JOIN", "PERIODIC", "COMMIT",
    "SCAN", "USING", "FALSE", "NULL", "TRUE", "ADD", "DO", "FOR", "MANDATORY", "OF", "REQUIRE",
    "SCALAR", "EXPLAIN", "PROFILE", "HEADERS", "FROM", "FIELDTERMINATOR", "STAR", "MINUS", "COUNT",
    "PRIMARY", "COPY"};

const string keywordColorPrefix = "\033[32m\033[1m";
const string keywordResetPostfix = "\033[00m";

const regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

vector<string> nodeTableNames;
vector<string> relTableNames;

void EmbeddedShell::updateTableNames() {
    nodeTableNames.clear();
    relTableNames.clear();
    for (auto& tableSchema : database->getCatalog()->getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTableNames.push_back(tableSchema.second->tableName);
    }
    for (auto& tableSchema : database->getCatalog()->getReadOnlyVersion()->getRelTableSchemas()) {
        relTableNames.push_back(tableSchema.second->tableName);
    }
}

void addTableCompletion(string buf, string tableName, linenoiseCompletions* lc) {
    string prefix, suffix;
    auto prefixPos = buf.rfind(':') + 1;
    prefix = buf.substr(0, prefixPos);
    suffix = buf.substr(prefixPos);
    if (suffix == tableName.substr(0, suffix.length())) {
        linenoiseAddCompletion(lc, (prefix + tableName).c_str());
    }
}

void completion(const char* buffer, linenoiseCompletions* lc) {
    string buf = string(buffer);

    // Command completion.
    if (buf[0] == ':') {
        for (auto& command : shellCommand.commandList) {
            if (regex_search(command, regex("^" + buf))) {
                linenoiseAddCompletion(lc, command.c_str());
            }
        }
        return;
    }

    // Node table name completion. Matches patterns that
    // include an open bracket `(` with no closing bracket
    // `)`, and a colon `:` sometime after the open bracket.
    if (regex_search(buf, regex("^[^]*\\([^\\)]*:[^\\)]*$"))) {
        for (auto& node : nodeTableNames) {
            addTableCompletion(buf, node, lc);
        }
        return;
    }

    // Rel table name completion. Matches patterns that
    // include an open square bracket `[` with no closing
    // bracket `]` and a colon `:` sometime after the open bracket.
    if (regex_search(buf, regex("^[^]*\\[[^\\]]*:[^\\]]*$"))) {
        for (auto& rel : relTableNames) {
            addTableCompletion(buf, rel, lc);
        }
        return;
    }

    // Keyword completion.
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
    updateTableNames();
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
            setNumThreads(lineStr.substr(shellCommand.THREAD.length()));
        } else if (lineStr.rfind(shellCommand.BUFFER_MANAGER_SIZE) == 0) {
            setBufferManagerSize(lineStr.substr(shellCommand.BUFFER_MANAGER_SIZE.length()));
        } else if (lineStr.rfind(shellCommand.BUFFER_MANAGER_DEBUG_INFO) == 0) {
            printf("Buffer Manager Debug Info: \n %s \n",
                database->getBufferManager()->debugInfo()->dump(4).c_str());
        } else if (lineStr.rfind(shellCommand.BUILT_IN_FUNCTIONS) == 0) {
            printf("%s", conn->getBuiltInFunctionNames().c_str());
        } else if (lineStr.rfind(shellCommand.LIST_NODES) == 0) {
            printf("%s", conn->getNodeTableNames().c_str());
        } else if (lineStr.rfind(shellCommand.LIST_RELS) == 0) {
            printf("%s", conn->getRelTableNames().c_str());
        } else if (lineStr.rfind(shellCommand.SHOW_NODE) == 0) {
            printNodeSchema(lineStr.substr(shellCommand.SHOW_NODE.length()));
        } else if (lineStr.rfind(shellCommand.SHOW_REL) == 0) {
            printRelSchema(lineStr.substr(shellCommand.SHOW_REL.length()));
        } else if (!lineStr.empty()) {
            auto queryResult = conn->query(lineStr);
            if (queryResult->isSuccess()) {
                printExecutionResult(*queryResult);
            } else {
                printf("Error: %s\n", queryResult->getErrorMessage().c_str());
            }
        }
        updateTableNames();

        linenoiseHistoryAdd(line);
        linenoiseHistorySave("history.txt");
        free(line);
    }
}

void EmbeddedShell::setNumThreads(const string& numThreadsString) {
    auto numThreads = 0;
    try {
        numThreads = stoi(numThreadsString);
    } catch (std::exception& e) {
        printf(
            "Cannot parse '%s' as number of threads. Expect integer.\n", numThreadsString.c_str());
    }
    try {
        conn->setMaxNumThreadForExec(numThreads);
        printf("numThreads set as %d\n", numThreads);
    } catch (Exception& e) { printf("%s", e.what()); }
}

void EmbeddedShell::setBufferManagerSize(const string& bufferManagerSizeString) {
    auto newPageSize = 0;
    try {
        newPageSize = stoull(bufferManagerSizeString);
    } catch (std::exception& e) {
        printf("Cannot parse '%s' as buffer manager size. Expect integer.\n", e.what());
    }
    try {
        database->resizeBufferManager(newPageSize);
    } catch (Exception& e) { printf("%s", e.what()); }
}

static inline string ltrim(const string& input) {
    auto s = input;
    s.erase(s.begin(), find_if(s.begin(), s.end(), [](unsigned char ch) { return !isspace(ch); }));
    return s;
}

void EmbeddedShell::printNodeSchema(const string& tableName) {
    auto name = ltrim(tableName);
    try {
        printf("%s\n", conn->getNodePropertyNames(name).c_str());
    } catch (Exception& e) { printf("%s\n", e.what()); }
}

void EmbeddedShell::printRelSchema(const string& tableName) {
    auto name = ltrim(tableName);
    try {
        printf("%s\n", conn->getRelPropertyNames(name).c_str());
    } catch (Exception& e) { printf("%s\n", e.what()); }
}

void EmbeddedShell::printHelp() {
    printf("%s%s %sget command list\n", TAB, shellCommand.HELP.c_str(), TAB);
    printf("%s%s %sclear shell\n", TAB, shellCommand.CLEAR.c_str(), TAB);
    printf("%s%s %sexit from shell\n", TAB, shellCommand.QUIT.c_str(), TAB);
    printf("%s%s [num_threads] %sset number of threads for execution\n", TAB,
        shellCommand.THREAD.c_str(), TAB);
    printf("%s%s [buffer_manager_size] %sset buffer manager size\n", TAB,
        shellCommand.BUFFER_MANAGER_SIZE.c_str(), TAB);
    printf("%s%s %sdebug information about the buffer manager\n", TAB,
        shellCommand.BUFFER_MANAGER_DEBUG_INFO.c_str(), TAB);
    printf("%s%s %slist built-in functions\n", TAB, shellCommand.BUILT_IN_FUNCTIONS.c_str(), TAB);
    printf("%s%s %slist all node tables\n", TAB, shellCommand.LIST_NODES.c_str(), TAB);
    printf("%s%s %slist all rel tables\n", TAB, shellCommand.LIST_RELS.c_str(), TAB);
    printf("%s%s %s[table_name] show node schema\n", TAB, shellCommand.SHOW_NODE.c_str(), TAB);
    printf("%s%s %s[table_name] show rel schema\n", TAB, shellCommand.SHOW_REL.c_str(), TAB);
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
        vector<uint32_t> colsWidth(queryResult.getNumColumns(), 2);
        for (auto i = 0u; i < colsWidth.size(); i++) {
            colsWidth[i] = queryResult.getColumnNames()[i].length() + 2;
        }
        uint32_t lineSeparatorLen = 1u + colsWidth.size();
        string lineSeparator;
        while (queryResult.hasNext()) {
            auto tuple = queryResult.getNext();
            for (auto i = 0u; i < colsWidth.size(); i++) {
                if (tuple->getResultValue(i)->isNullVal()) {
                    continue;
                }
                uint32_t fieldLen = tuple->getResultValue(i)->to_string().length() + 2;
                colsWidth[i] =
                    max(colsWidth[i], (fieldLen > colsWidth[i]) ? fieldLen : colsWidth[i]);
            }
        }
        for (auto width : colsWidth) {
            lineSeparatorLen += width;
        }
        lineSeparator = string(lineSeparatorLen, '-');
        printf("%s\n", lineSeparator.c_str());

        if (queryResult.getColumnNames()[0] != "") {
            for (auto i = 0u; i < colsWidth.size(); i++) {
                printf("| %s", queryResult.getColumnNames()[i].c_str());
                printf(
                    "%s", string(colsWidth[i] - queryResult.getColumnNames()[i].length() - 1, ' ')
                              .c_str());
            }
            printf("|\n");
            printf("%s\n", lineSeparator.c_str());
        }

        queryResult.resetIterator();
        while (queryResult.hasNext()) {
            auto tuple = queryResult.getNext();
            printf("|%s|\n", tuple->toString(colsWidth, "|").c_str());
            printf("%s\n", lineSeparator.c_str());
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
