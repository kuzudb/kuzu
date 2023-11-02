#include "embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <csignal>
#include <regex>
#include <sstream>

#include "catalog/catalog.h"
#include "common/string_utils.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::common;
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
    const std::string HELP = ":help";
    const std::string CLEAR = ":clear";
    const std::string QUIT = ":quit";
    const std::string THREAD = ":thread";
    const std::string LOGGING_LEVEL = ":logging_level";
    const std::string QUERY_TIMEOUT = ":timeout";
    const std::vector<std::string> commandList = {
        HELP, CLEAR, QUIT, THREAD, LOGGING_LEVEL, QUERY_TIMEOUT};
} shellCommand;

const char* TAB = "    ";

const std::vector<std::string> keywordList = {"CALL", "CREATE", "DELETE", "DETACH", "EXISTS",
    "FOREACH", "LOAD", "MATCH", "MERGE", "OPTIONAL", "REMOVE", "RETURN", "SET", "START", "UNION",
    "UNWIND", "WITH", "LIMIT", "ORDER", "SKIP", "WHERE", "YIELD", "ASC", "ASCENDING", "ASSERT",
    "BY", "CSV", "DESC", "DESCENDING", "ON", "ALL", "CASE", "ELSE", "END", "THEN", "WHEN", "AND",
    "AS", "REL", "TABLE", "CONTAINS", "DISTINCT", "ENDS", "IN", "IS", "NOT", "OR", "STARTS", "XOR",
    "CONSTRAINT", "DROP", "EXISTS", "INDEX", "NODE", "KEY", "UNIQUE", "INDEX", "JOIN", "PERIODIC",
    "COMMIT", "SCAN", "USING", "FALSE", "NULL", "TRUE", "ADD", "DO", "FOR", "MANDATORY", "OF",
    "REQUIRE", "SCALAR", "EXPLAIN", "PROFILE", "HEADERS", "FROM", "FIELDTERMINATOR", "STAR",
    "MINUS", "COUNT", "PRIMARY", "COPY"};

const std::string keywordColorPrefix = "\033[32m\033[1m";
const std::string keywordResetPostfix = "\033[00m";

const std::regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

std::vector<std::string> nodeTableNames;
std::vector<std::string> relTableNames;

bool continueLine = false;
std::string currLine;

static Connection* globalConnection;

void EmbeddedShell::updateTableNames() {
    nodeTableNames.clear();
    relTableNames.clear();
    for (auto& tableSchema : database->catalog->getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTableNames.push_back(tableSchema->tableName);
    }
    for (auto& tableSchema : database->catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        relTableNames.push_back(tableSchema->tableName);
    }
}

void addTableCompletion(std::string buf, const std::string& tableName, linenoiseCompletions* lc) {
    std::string prefix, suffix;
    auto prefixPos = buf.rfind(':') + 1;
    prefix = buf.substr(0, prefixPos);
    suffix = buf.substr(prefixPos);
    if (suffix == tableName.substr(0, suffix.length())) {
        linenoiseAddCompletion(lc, (prefix + tableName).c_str());
    }
}

void completion(const char* buffer, linenoiseCompletions* lc) {
    std::string buf = std::string(buffer);

    // Command completion.
    if (buf[0] == ':') {
        for (auto& command : shellCommand.commandList) {
            if (regex_search(command, std::regex("^" + buf))) {
                linenoiseAddCompletion(lc, command.c_str());
            }
        }
        return;
    }

    // Node table name completion. Match patterns that include an open bracket `(` with no closing
    // bracket `)`, and a colon `:` sometime after the open bracket.
    if (regex_search(buf, std::regex("^[^]*\\([^\\)]*:[^\\)]*$"))) {
        for (auto& node : nodeTableNames) {
            addTableCompletion(buf, node, lc);
        }
        return;
    }

    // Rel table name completion. Matches patterns that
    // include an open square bracket `[` with no closing
    // bracket `]` and a colon `:` sometime after the open bracket.
    if (regex_search(buf, std::regex("^[^]*\\[[^\\]]*:[^\\]]*$"))) {
        for (auto& rel : relTableNames) {
            addTableCompletion(buf, rel, lc);
        }
        return;
    }

    // Keyword completion.
    std::string prefix;
    auto lastKeywordPos = buf.rfind(' ') + 1;
    if (lastKeywordPos != std::string::npos) {
        prefix = buf.substr(0, lastKeywordPos);
        buf = buf.substr(lastKeywordPos);
    }
    if (buf.empty()) {
        return;
    }
    for (std::string keyword : keywordList) {
        std::string bufEscaped = regex_replace(buf, specialChars, R"(\$&)");
        if (regex_search(keyword, std::regex("^" + bufEscaped, std::regex_constants::icase))) {
            if (islower(buf[0])) {
                transform(keyword.begin(), keyword.end(), keyword.begin(), ::tolower);
            }
            linenoiseAddCompletion(lc, (prefix + keyword).c_str());
        }
    }
}

void highlight(char* buffer, char* resultBuf, uint32_t renderWidth, uint32_t cursorPos) {
    std::string buf(buffer);
    auto bufLen = buf.length();
    std::ostringstream highlightBuffer;
    std::string word;
    std::vector<std::string> tokenList;
    if (cursorPos > renderWidth) {
        uint32_t counter = 0;
        uint32_t thisChar = 0;
        uint32_t lineLen = 0;
        while (counter < cursorPos) {
            counter += Utf8Proc::renderWidth(buffer, thisChar);
            thisChar = utf8proc_next_grapheme(buffer, bufLen, thisChar);
        }
        lineLen = thisChar;
        while (counter > cursorPos - renderWidth + 1) {
            counter -= Utf8Proc::renderWidth(buffer, thisChar);
            thisChar = Utf8Proc::previousGraphemeCluster(buffer, bufLen, thisChar);
        }
        lineLen -= thisChar;
        buf = buf.substr(thisChar, lineLen);
    } else if (buf.length() > renderWidth) {
        uint32_t counter = 0;
        uint32_t lineLen = 0;
        while (counter < renderWidth) {
            counter += Utf8Proc::renderWidth(buffer, lineLen);
            lineLen = utf8proc_next_grapheme(buffer, bufLen, lineLen);
        }
        buf = buf.substr(0, lineLen);
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
    for (std::string& token : tokenList) {
#ifndef _WIN32
        if (token.find(' ') == std::string::npos) {
            for (const std::string& keyword : keywordList) {
                if (regex_search(
                        token, std::regex("^" + keyword + "$", std::regex_constants::icase)) ||
                    regex_search(
                        token, std::regex("^" + keyword + "\\(", std::regex_constants::icase))) {
                    token = regex_replace(token,
                        std::regex("^(" + keyword + ")", std::regex_constants::icase),
                        keywordColorPrefix + "$1" + keywordResetPostfix);
                    break;
                }
            }
        }
#endif
        highlightBuffer << token;
    }
    // Linenoise allocates a fixed size buffer for the current line's contents, and doesn't export
    // the length.
    constexpr size_t LINENOISE_MAX_LINE = 4096;
    strncpy(resultBuf, highlightBuffer.str().c_str(), LINENOISE_MAX_LINE - 1);
}

EmbeddedShell::EmbeddedShell(const std::string& databasePath, const SystemConfig& systemConfig) {
    linenoiseHistoryLoad(HISTORY_PATH);
    linenoiseSetCompletionCallback(completion);
    linenoiseSetHighlightCallback(highlight);
    database = std::make_unique<Database>(databasePath, systemConfig);
    conn = std::make_unique<Connection>(database.get());
    globalConnection = conn.get();
    updateTableNames();
    signal(SIGINT, interruptHandler);
}

void EmbeddedShell::run() {
    char* line;
    std::string query;
    std::stringstream ss;
    while ((line = linenoise(continueLine ? ALTPROMPT : PROMPT)) != nullptr) {
        auto lineStr = std::string(line);
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
        } else if (lineStr.rfind(shellCommand.LOGGING_LEVEL) == 0) {
            setLoggingLevel(lineStr.substr(shellCommand.LOGGING_LEVEL.length()));
        } else if (lineStr.rfind(shellCommand.QUERY_TIMEOUT) == 0) {
            setQueryTimeout(lineStr.substr(shellCommand.QUERY_TIMEOUT.length()));
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

void EmbeddedShell::interruptHandler(int /*signal*/) {
    globalConnection->interrupt();
}

void EmbeddedShell::setNumThreads(const std::string& numThreadsString) {
    auto numThreads = 0;
    try {
        numThreads = stoi(numThreadsString);
    } catch (std::exception& e) {
        printf(
            "Cannot parse '%s' as number of threads. Expect integer.\n", numThreadsString.c_str());
        return;
    }
    try {
        conn->setMaxNumThreadForExec(numThreads);
        printf("numThreads set as %d\n", numThreads);
    } catch (Exception& e) { printf("%s", e.what()); }
}

void EmbeddedShell::printHelp() {
    printf("%s%s %sget command list\n", TAB, shellCommand.HELP.c_str(), TAB);
    printf("%s%s %sclear shell\n", TAB, shellCommand.CLEAR.c_str(), TAB);
    printf("%s%s %sexit from shell\n", TAB, shellCommand.QUIT.c_str(), TAB);
    printf("%s%s [num_threads] %sset number of threads for query execution\n", TAB,
        shellCommand.THREAD.c_str(), TAB);
    printf("%s%s [logging_level] %sset logging level of database, available options: debug, info, "
           "err\n",
        TAB, shellCommand.LOGGING_LEVEL.c_str(), TAB);
    printf("%s%s [query_timeout] %sset query timeout in ms\n", TAB,
        shellCommand.QUERY_TIMEOUT.c_str(), TAB);
}

void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->isExplain()) {
        printf("%s", queryResult.getNext()->toString().c_str());
    } else {
        const uint32_t maxWidth = 80;
        uint64_t numTuples = queryResult.getNumTuples();
        std::vector<uint32_t> colsWidth(queryResult.getNumColumns(), 2);
        for (auto i = 0u; i < colsWidth.size(); i++) {
            colsWidth[i] = queryResult.getColumnNames()[i].length() + 2;
        }
        uint32_t lineSeparatorLen = 1u + colsWidth.size();
        std::string lineSeparator;
        while (queryResult.hasNext()) {
            auto tuple = queryResult.getNext();
            for (auto i = 0u; i < colsWidth.size(); i++) {
                if (tuple->getValue(i)->isNull()) {
                    continue;
                }
                std::string tupleString = tuple->getValue(i)->toString();
                uint32_t fieldLen = 0;
                uint32_t chrIter = 0;
                while (chrIter < tupleString.length()) {
                    fieldLen += Utf8Proc::renderWidth(tupleString.c_str(), chrIter);
                    chrIter =
                        utf8proc_next_grapheme(tupleString.c_str(), tupleString.length(), chrIter);
                }
                // An extra 2 spaces are added for an extra space on either
                // side of the std::string.
                colsWidth[i] = std::max(colsWidth[i], std::min(fieldLen, maxWidth) + 2);
            }
        }
        for (auto width : colsWidth) {
            lineSeparatorLen += width;
        }
        lineSeparator = std::string(lineSeparatorLen, '-');
        printf("%s\n", lineSeparator.c_str());

        if (queryResult.getNumColumns() != 0 && !queryResult.getColumnNames()[0].empty()) {
            for (auto i = 0u; i < colsWidth.size(); i++) {
                printf("| %s", queryResult.getColumnNames()[i].c_str());
                printf("%s",
                    std::string(colsWidth[i] - queryResult.getColumnNames()[i].length() - 1, ' ')
                        .c_str());
            }
            printf("|\n");
            printf("%s\n", lineSeparator.c_str());
        }

        queryResult.resetIterator();
        while (queryResult.hasNext()) {
            auto tuple = queryResult.getNext();
            printf("|%s|\n", tuple->toString(colsWidth, "|", maxWidth).c_str());
            printf("%s\n", lineSeparator.c_str());
        }

        // print query result (numFlatTuples & tuples)
        if (numTuples == 1) {
            printf("(1 tuple)\n");
        } else {
            printf("(%" PRIu64 " tuples)\n", numTuples);
        }
        printf("Time: %.2fms (compiling), %.2fms (executing)\n", querySummary->getCompilingTime(),
            querySummary->getExecutionTime());
    }
}

void EmbeddedShell::setLoggingLevel(const std::string& loggingLevel) {
    auto level = StringUtils::ltrim(loggingLevel);
    try {
        database->setLoggingLevel(level);
        printf("logging level has been set to: %s.\n", level.c_str());
    } catch (Exception& e) { printf("%s", e.what()); }
}

void EmbeddedShell::setQueryTimeout(const std::string& timeoutInMS) {
    auto queryTimeOutVal = std::stoull(StringUtils::ltrim(timeoutInMS));
    conn->setQueryTimeOut(queryTimeOutVal);
    printf("query timeout value has been set to: %llu ms.\n", queryTimeOutVal);
}

} // namespace main
} // namespace kuzu
