#include "embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <regex>

#include "common/logging_level_utils.h"
#include "common/type_utils.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace main {

// prompt for user input
const char* PROMPT = "kuzu> ";
const char* ALTPROMPT = "..> ";
// file to read/write shell history
const char* HISTORY_PATH = "history.txt";

// build-in shell command
struct ShellCommand {
    const string HELP = ":help";
    const string CLEAR = ":clear";
    const string QUIT = ":quit";
    const string THREAD = ":thread";
    const string BUFFER_MANAGER_SIZE = ":buffer_manager_size";
    const string LIST_NODES = ":list_nodes";
    const string LIST_RELS = ":list_rels";
    const string SHOW_NODE = ":show_node";
    const string SHOW_REL = ":show_rel";
    const string LOGGING_LEVEL = ":logging_level";
    const vector<string> commandList = {HELP, CLEAR, QUIT, THREAD, BUFFER_MANAGER_SIZE, LIST_NODES,
        LIST_RELS, SHOW_NODE, SHOW_REL, LOGGING_LEVEL};
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

bool continueLine = false;
string currLine;

void EmbeddedShell::updateTableNames() {
    nodeTableNames.clear();
    relTableNames.clear();
    for (auto& tableSchema : database->catalog->getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTableNames.push_back(tableSchema.second->tableName);
    }
    for (auto& tableSchema : database->catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        relTableNames.push_back(tableSchema.second->tableName);
    }
}

void addTableCompletion(string buf, const string& tableName, linenoiseCompletions* lc) {
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

    // Node table name completion. Match patterns that include an open bracket `(` with no closing
    // bracket `)`, and a colon `:` sometime after the open bracket.
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
    auto bufLen = buf.length();
    ostringstream highlightBuffer;
    string word;
    vector<string> tokenList;
    if (cursorPos > maxLen) {
        uint32_t counter = 0;
        uint32_t thisChar = 0;
        uint32_t lineLen = 0;
        while (counter < cursorPos) {
            counter += Utf8Proc::renderWidth(buffer, thisChar);
            thisChar = utf8proc_next_grapheme(buffer, bufLen, thisChar);
        }
        lineLen = thisChar;
        while (counter > cursorPos - maxLen + 1) {
            counter -= Utf8Proc::renderWidth(buffer, thisChar);
            thisChar = Utf8Proc::previousGraphemeCluster(buffer, bufLen, thisChar);
        }
        lineLen -= thisChar;
        buf = buf.substr(thisChar, lineLen);
        bufLen = buf.length();
    } else if (buf.length() > maxLen) {
        uint32_t counter = 0;
        uint32_t lineLen = 0;
        while (counter < maxLen) {
            counter += Utf8Proc::renderWidth(buffer, lineLen);
            lineLen = utf8proc_next_grapheme(buffer, bufLen, lineLen);
        }
        buf = buf.substr(0, lineLen);
        bufLen = buf.length();
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
            for (const string& keyword : keywordList) {
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
    string query;
    stringstream ss;
    while ((line = linenoise(continueLine ? ALTPROMPT : PROMPT)) != nullptr) {
        auto lineStr = string(line);
        if (continueLine) {
            lineStr = currLine + lineStr;
            currLine = "";
            continueLine = false;
        }
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
        } else if (lineStr.rfind(shellCommand.LIST_NODES) == 0) {
            printf("%s", conn->getNodeTableNames().c_str());
        } else if (lineStr.rfind(shellCommand.LIST_RELS) == 0) {
            printf("%s", conn->getRelTableNames().c_str());
        } else if (lineStr.rfind(shellCommand.SHOW_NODE) == 0) {
            printNodeSchema(lineStr.substr(shellCommand.SHOW_NODE.length()));
        } else if (lineStr.rfind(shellCommand.SHOW_REL) == 0) {
            printRelSchema(lineStr.substr(shellCommand.SHOW_REL.length()));
        } else if (lineStr.rfind(shellCommand.LOGGING_LEVEL) == 0) {
            setLoggingLevel(lineStr.substr(shellCommand.LOGGING_LEVEL.length()));
        } else if (!lineStr.empty()) {
            ss.clear();
            ss.str(lineStr);
            while (getline(ss, query, ';')) {
                if (ss.eof() && ss.peek() == -1) {
                    continueLine = true;
                    currLine += query + " ";
                } else {
                    auto queryResult = conn->query(query);
                    if (queryResult->isSuccess()) {
                        printExecutionResult(*queryResult);
                    } else {
                        printf("Error: %s\n", queryResult->getErrorMessage().c_str());
                    }
                }
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
    } catch (Exception& e) { printf("%s\n", e.what()); }
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
    printf("%s%s [num_threads] %sset number of threads for query execution\n", TAB,
        shellCommand.THREAD.c_str(), TAB);
    printf("%s%s [bm_size_in_bytes] %sset buffer manager size in bytes\n", TAB,
        shellCommand.BUFFER_MANAGER_SIZE.c_str(), TAB);
    printf("%s%s [logging_level] %sset logging level of database, available options: debug, info, "
           "err\n",
        TAB, shellCommand.LOGGING_LEVEL.c_str(), TAB);
    printf("%s%s %slist all node tables\n", TAB, shellCommand.LIST_NODES.c_str(), TAB);
    printf("%s%s %slist all rel tables\n", TAB, shellCommand.LIST_RELS.c_str(), TAB);
    printf("%s%s %s[table_name] show node schema\n", TAB, shellCommand.SHOW_NODE.c_str(), TAB);
    printf("%s%s %s[table_name] show rel schema\n", TAB, shellCommand.SHOW_REL.c_str(), TAB);
}

void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->getIsExplain()) {
        auto& oss = querySummary->getPlanAsOstream();
        printf("%s", oss.str().c_str());
    } else {
        uint64_t numTuples = queryResult.getNumTuples();
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
                uint32_t fieldLen = tuple->getResultValue(i)->toString().length() + 2;
                colsWidth[i] =
                    max(colsWidth[i], (fieldLen > colsWidth[i]) ? fieldLen : colsWidth[i]);
            }
        }
        for (auto width : colsWidth) {
            lineSeparatorLen += width;
        }
        lineSeparator = string(lineSeparatorLen, '-');
        printf("%s\n", lineSeparator.c_str());

        if (queryResult.getNumColumns() != 0 && !queryResult.getColumnNames()[0].empty()) {
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

        // print query result (numFlatTuples & tuples)
        if (numTuples == 1) {
            printf("(1 tuple)\n");
        } else {
            printf("(%llu tuples)\n", numTuples);
        }
        printf("Time: %.2fms (compiling), %.2fms (executing)\n", querySummary->getCompilingTime(),
            querySummary->getExecutionTime());

        if (querySummary->getIsProfile()) {
            // print plan with profiling metrics
            printf("==============================================\n");
            printf("=============== Profiler Summary =============\n");
            printf("==============================================\n");
            printf(">> plan\n");
            printf("%s", querySummary->getPlanAsOstream().str().c_str());
        }
    }
}

void EmbeddedShell::setLoggingLevel(const string& loggingLevel) {
    auto level = ltrim(loggingLevel);
    try {
        auto logLevelEnum = LoggingLevelUtils::convertStrToLevelEnum(level);
        database->setLoggingLevel(logLevelEnum);
        printf("logging level has been set to: %s.\n", level.c_str());
    } catch (ConversionException& e) { printf("%s", e.what()); }
}

} // namespace main
} // namespace kuzu
