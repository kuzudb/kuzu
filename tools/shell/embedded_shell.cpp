#include "embedded_shell.h"

#ifndef _WIN32
#include <termios.h>
#include <unistd.h>
#else
#include <windows.h>
#endif
#include <algorithm>
#include <array>
#include <cctype>
#include <csignal>
#include <regex>
#include <sstream>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/parser.h"
#include "keywords.h"
#include "transaction/transaction.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::common;
using namespace kuzu::utf8proc;

namespace kuzu {
namespace main {

#ifdef _WIN32
#ifndef STDIN_FILENO
#define STDIN_FILENO (_fileno(stdin))
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO (_fileno(stdout))
#endif
#endif

// prompt for user input
const char* PROMPT = "kuzu> ";
const char* ALTPROMPT = "..> ";

// build-in shell command
struct ShellCommand {
    const char* HELP = ":help";
    const char* CLEAR = ":clear";
    const char* QUIT = ":quit";
    const char* MAX_ROWS = ":max_rows";
    const char* MAX_WIDTH = ":max_width";
    const std::array<const char*, 5> commandList = {HELP, CLEAR, QUIT, MAX_ROWS, MAX_WIDTH};
} shellCommand;

const char* TAB = "    ";

const std::array<const char*, _keywordListLength> keywordList = _keywordList;

const char* keywordColorPrefix = "\033[32m\033[1m";
const char* keywordResetPostfix = "\033[39m\033[22m";

// NOLINTNEXTLINE(cert-err58-cpp): OK to have a global regex, even if the constructor allocates.
const std::regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

std::vector<std::string> nodeTableNames;
std::vector<std::string> relTableNames;

bool continueLine = false;
std::string currLine;

const int defaultMaxRows = 20;

static Connection* globalConnection;

#ifndef _WIN32
struct termios orig_termios;
bool noEcho = false;
#else
DWORD oldOutputCP;
#endif

void EmbeddedShell::updateTableNames() {
    nodeTableNames.clear();
    relTableNames.clear();
    for (auto& nodeTableEntry :
        database->catalog->getNodeTableEntries(&transaction::DUMMY_READ_TRANSACTION)) {
        nodeTableNames.push_back(nodeTableEntry->getName());
    }
    for (auto& relTableEntry :
        database->catalog->getRelTableEntries(&transaction::DUMMY_READ_TRANSACTION)) {
        relTableNames.push_back(relTableEntry->getName());
    }
}

void addTableCompletion(const std::string& buf, const std::string& tableName,
    linenoiseCompletions* lc) {
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
                linenoiseAddCompletion(lc, command);
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
        if (token.find(' ') == std::string::npos) {
            for (const std::string keyword : keywordList) {
                if (regex_search(token,
                        std::regex("^" + keyword + "$", std::regex_constants::icase)) ||
                    regex_search(token,
                        std::regex("^" + keyword + "\\(", std::regex_constants::icase)) ||
                    regex_search(token,
                        std::regex("\\(" + keyword + "$", std::regex_constants::icase))) {
                    token = regex_replace(token,
                        std::regex(std::string("(").append(keyword).append(")"),
                            std::regex_constants::icase),
                        std::string(keywordColorPrefix).append("$1").append(keywordResetPostfix));
                    break;
                }
            }
        }
        highlightBuffer << token;
    }
    // Linenoise allocates a fixed size buffer for the current line's contents, and doesn't export
    // the length.
    constexpr uint64_t LINENOISE_MAX_LINE = 4096;
    strncpy(resultBuf, highlightBuffer.str().c_str(), LINENOISE_MAX_LINE - 1);
}

uint64_t damerauLevenshteinDistance(const std::string& s1, const std::string& s2) {
    const uint64_t m = s1.size(), n = s2.size();
    std::vector<std::vector<uint64_t>> dp(m + 1, std::vector<uint64_t>(n + 1, 0));
    for (uint64_t i = 0; i <= m; i++) {
        dp[i][0] = i;
    }
    for (uint64_t j = 0; j <= n; j++) {
        dp[0][j] = j;
    }
    for (uint64_t i = 1; i <= m; i++) {
        for (uint64_t j = 1; j <= n; j++) {
            if (s1[i - 1] == s2[j - 1]) {
                dp[i][j] = dp[i - 1][j - 1];
                if (i > 1 && j > 1 && s1[i - 1] == s2[j - 2] && s1[i - 2] == s2[j - 1]) {
                    dp[i][j] = std::min(dp[i][j], dp[i - 2][j - 2]);
                }
            } else {
                dp[i][j] = 1 + std::min({dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]});
                if (i > 1 && j > 1 && s1[i - 1] == s2[j - 2] && s1[i - 2] == s2[j - 1]) {
                    dp[i][j] = std::min(dp[i][j], dp[i - 2][j - 2] + 1);
                }
            }
        }
    }
    return dp[m][n];
}

std::string findClosestCommand(std::string lineStr) {
    std::string closestCommand = "";
    uint64_t editDistance = INT_MAX;
    for (auto& command : shellCommand.commandList) {
        auto distance = damerauLevenshteinDistance(command, lineStr);
        if (distance < editDistance) {
            editDistance = distance;
            closestCommand = command;
        }
    }
    return closestCommand;
}

int EmbeddedShell::processShellCommands(std::string lineStr) {
    if (lineStr == shellCommand.HELP) {
        printHelp();
    } else if (lineStr == shellCommand.CLEAR) {
        linenoiseClearScreen();
    } else if (lineStr == shellCommand.QUIT) {
        return -1;
    } else if (lineStr.rfind(shellCommand.MAX_ROWS) == 0) {
        setMaxRows(lineStr.substr(strlen(shellCommand.MAX_ROWS)));
    } else if (lineStr.rfind(shellCommand.MAX_WIDTH) == 0) {
        setMaxWidth(lineStr.substr(strlen(shellCommand.MAX_WIDTH)));
    } else {
        printf("Error: Unknown command: \"%s\". Enter \":help\" for help\n", lineStr.c_str());
        printf("Did you mean: \"%s\"?\n", findClosestCommand(lineStr).c_str());
    }
    return 0;
}

EmbeddedShell::EmbeddedShell(std::shared_ptr<Database> database, std::shared_ptr<Connection> conn,
    const char* pathToHistory) {
    path_to_history = pathToHistory;
    linenoiseHistoryLoad(path_to_history);
    linenoiseSetCompletionCallback(completion);
    linenoiseSetHighlightCallback(highlight);
    this->database = database;
    this->conn = conn;
    globalConnection = conn.get();
    maxRowSize = defaultMaxRows;
    maxPrintWidth = 0; // Will be determined when printing
    updateTableNames();
    KU_ASSERT(signal(SIGINT, interruptHandler) != SIG_ERR);
}

void EmbeddedShell::run() {
    char* line;
    std::string query;
    std::stringstream ss;
    const char ctrl_c = '\3';
    int numCtrlC = 0;

#ifndef _WIN32
    struct termios raw;
    if (isatty(STDIN_FILENO)) {
        if (tcgetattr(STDIN_FILENO, &orig_termios) == -1) {
            errno = ENOTTY;
            return;
        }
        raw = orig_termios;
        raw.c_lflag &= ~ECHO;

        if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) < 0) {
            errno = ENOTTY;
            return;
        }
        noEcho = true;
    }
#else
    oldOutputCP = GetConsoleOutputCP();
    SetConsoleOutputCP(CP_UTF8);
#endif

    while ((line = linenoise(continueLine ? ALTPROMPT : PROMPT)) != nullptr) {
        auto lineStr = std::string(line);
        lineStr = lineStr.erase(lineStr.find_last_not_of(" \t\n\r\f\v") + 1);
        if (!lineStr.empty() && lineStr[0] == ctrl_c) {
            if (!continueLine && lineStr[1] == '\0') {
                numCtrlC++;
                if (numCtrlC >= 2) {
                    free(line);
                    break;
                }
            }
            currLine = "";
            continueLine = false;
            free(line);
            continue;
        }
        numCtrlC = 0;
        if (continueLine) {
            lineStr = std::move(currLine) + std::move(lineStr);
            currLine = "";
            continueLine = false;
        }
        if (!continueLine && lineStr[0] == ':' && processShellCommands(lineStr) < 0) {
            free(line);
            break;
        } else if (!lineStr.empty() && lineStr.back() == ';') {
            ss.clear();
            ss.str(lineStr);
            while (getline(ss, query, ';')) {
                auto queryResult = conn->query(query);
                if (queryResult->isSuccess()) {
                    printExecutionResult(*queryResult);
                } else {
                    std::string errMsg = queryResult->getErrorMessage();
                    printf("Error: %s\n", errMsg.c_str());
                    if (errMsg.find(ParserException::ERROR_PREFIX) == 0) {
                        std::string trimmedLineStr = lineStr;
                        trimmedLineStr.erase(0, trimmedLineStr.find_first_not_of(" \t\n\r\f\v"));
                        if (trimmedLineStr.find(' ') == std::string::npos) {
                            printf("\"%s\" is not a valid Cypher query. Did you mean to issue a "
                                   "CLI command, e.g., \"%s\"?\n",
                                lineStr.c_str(), findClosestCommand(lineStr).c_str());
                        }
                    }
                }
            }
        } else if (!lineStr.empty() && lineStr[0] != ':') {
            continueLine = true;
            currLine += lineStr + " ";
        }
        updateTableNames();

        if (!continueLine) {
            linenoiseHistoryAdd(lineStr.c_str());
        }
        linenoiseHistorySave(path_to_history);
        free(line);
    }
#ifndef _WIN32
    /* Don't even check the return value as it's too late. */
    if (noEcho && tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios) != -1)
        noEcho = false;
#else
    SetConsoleOutputCP(oldOutputCP);
#endif
}

void EmbeddedShell::interruptHandler(int /*signal*/) {
    globalConnection->interrupt();
#ifndef _WIN32
    /* Don't even check the return value as it's too late. */
    if (noEcho && tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios) != -1)
        noEcho = false;
#else
    SetConsoleOutputCP(oldOutputCP);
#endif
}

void EmbeddedShell::setMaxRows(const std::string& maxRowsString) {
    uint64_t parsedMaxRows = 0;
    try {
        parsedMaxRows = stoull(maxRowsString);
    } catch (std::exception& e) {
        printf("Cannot parse '%s' as number of rows. Expect integer.\n", maxRowsString.c_str());
        return;
    }
    maxRowSize = parsedMaxRows == 0 ? defaultMaxRows : parsedMaxRows;
    printf("maxRows set as %" PRIu64 "\n", maxRowSize);
}

void EmbeddedShell::setMaxWidth(const std::string& maxWidthString) {
    uint32_t parsedMaxWidth = 0;
    try {
        parsedMaxWidth = stoul(maxWidthString);
    } catch (std::exception& e) {
        printf("Cannot parse '%s' as number of characters. Expect integer.\n",
            maxWidthString.c_str());
        return;
    }
    maxPrintWidth = parsedMaxWidth;
    printf("maxWidth set as %d\n", parsedMaxWidth);
}

void EmbeddedShell::printHelp() {
    printf("%s%s %sget command list\n", TAB, shellCommand.HELP, TAB);
    printf("%s%s %sclear shell\n", TAB, shellCommand.CLEAR, TAB);
    printf("%s%s %sexit from shell\n", TAB, shellCommand.QUIT, TAB);
    printf("%s%s [max_rows] %sset maximum number of rows for display (default: 20)\n", TAB,
        shellCommand.MAX_ROWS, TAB);
    printf("%s%s [max_width] %sset maximum width in characters for display\n", TAB,
        shellCommand.MAX_WIDTH, TAB);
    printf("\n");
    printf("%sNote: you can change and see several system configurations, such as num-threads, \n",
        TAB);
    printf("%s%s  timeout, and progress_bar using Cypher CALL statements.\n", TAB, TAB);
    printf("%s%s  e.g. CALL THREADS=5; or CALL current_setting('threads') return *;\n", TAB, TAB);
    printf("%s%s  See: https://docs.kuzudb.com/cypher/configuration\n", TAB, TAB);
}

void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->isExplain()) {
        printf("%s", queryResult.getNext()->toString().c_str());
    } else {
        constexpr uint32_t SMALL_TABLE_SEPERATOR_LENGTH = 3;
        const uint32_t minTruncatedWidth = 20;
        uint64_t numTuples = queryResult.getNumTuples();
        std::vector<uint32_t> colsWidth(queryResult.getNumColumns(), 2);
        for (auto i = 0u; i < colsWidth.size(); i++) {
            colsWidth[i] = queryResult.getColumnNames()[i].length() + 2;
        }
        std::string lineSeparator;
        uint64_t rowCount = 0;
        while (queryResult.hasNext()) {
            if (numTuples > maxRowSize && rowCount >= (maxRowSize / 2) + (maxRowSize % 2 != 0) &&
                rowCount < numTuples - maxRowSize / 2) {
                auto tuple = queryResult.getNext();
                rowCount++;
                continue;
            }
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
                colsWidth[i] = std::max(colsWidth[i], fieldLen + 2);
            }
            rowCount++;
        }

        uint32_t sumGoal = minTruncatedWidth;
        uint32_t maxWidth = minTruncatedWidth;
        if (colsWidth.size() == 1) {
            uint32_t minDisplayWidth = minTruncatedWidth + SMALL_TABLE_SEPERATOR_LENGTH;
            if (maxPrintWidth > minDisplayWidth) {
                sumGoal = maxPrintWidth - 2;
            } else {
                sumGoal = std::max(
                    (uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
                    minDisplayWidth);
            }
        } else if (colsWidth.size() > 1) {
            uint32_t minDisplayWidth = SMALL_TABLE_SEPERATOR_LENGTH + minTruncatedWidth * 2;
            if (maxPrintWidth > minDisplayWidth) {
                sumGoal = maxPrintWidth - colsWidth.size() - 1;
            } else {
                // make sure there is space for the first and last column
                sumGoal = std::max(
                    (uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
                    minDisplayWidth);
            }
        } else if (maxPrintWidth > minTruncatedWidth) {
            sumGoal = maxPrintWidth;
        }
        uint32_t sum = 0;
        std::vector<uint32_t> maxValueIndex;
        uint32_t secondHighestValue = 0;
        for (auto i = 0u; i < colsWidth.size(); i++) {
            if (maxValueIndex.empty() || colsWidth[i] == colsWidth[maxValueIndex[0]]) {
                maxValueIndex.push_back(i);
                maxWidth = colsWidth[maxValueIndex[0]];
            } else if (colsWidth[i] > colsWidth[maxValueIndex[0]]) {
                secondHighestValue = colsWidth[maxValueIndex[0]];
                maxValueIndex.clear();
                maxValueIndex.push_back(i);
                maxWidth = colsWidth[maxValueIndex[0]];
            } else if (colsWidth[i] > secondHighestValue) {
                secondHighestValue = colsWidth[i];
            }
            sum += colsWidth[i];
        }

        while (sum > sumGoal) {
            uint32_t truncationValue = ((sum - sumGoal) / maxValueIndex.size()) +
                                       ((sum - sumGoal) % maxValueIndex.size() != 0);
            uint32_t newValue = 0;
            if (truncationValue < colsWidth[maxValueIndex[0]]) {
                newValue = colsWidth[maxValueIndex[0]] - truncationValue;
            }
            uint32_t oldValue = colsWidth[maxValueIndex[0]];
            if (secondHighestValue < minTruncatedWidth + 2 && newValue < minTruncatedWidth + 2) {
                newValue = minTruncatedWidth + 2;
            } else {
                uint32_t sumDifference =
                    sum - ((oldValue - secondHighestValue) * maxValueIndex.size());
                if (sumDifference > sumGoal) {
                    newValue = secondHighestValue;
                }
            }
            for (auto i = 0u; i < maxValueIndex.size(); i++) {
                colsWidth[maxValueIndex[i]] = newValue;
            }
            maxWidth = newValue - 2;
            sum -= (oldValue - newValue) * maxValueIndex.size();
            if (newValue == minTruncatedWidth + 2) {
                break;
            }

            maxValueIndex.clear();
            secondHighestValue = 0;
            for (auto i = 0u; i < colsWidth.size(); i++) {
                if (maxValueIndex.empty() || colsWidth[i] == colsWidth[maxValueIndex[0]]) {
                    maxValueIndex.push_back(i);
                } else if (colsWidth[i] > colsWidth[maxValueIndex[0]]) {
                    secondHighestValue = colsWidth[maxValueIndex[0]];
                    maxValueIndex.clear();
                    maxValueIndex.push_back(i);
                } else if (colsWidth[i] > secondHighestValue) {
                    secondHighestValue = colsWidth[i];
                }
            }
        }

        int k = 0;
        int j = colsWidth.size() - 1;
        bool colTruncated = false;
        uint64_t colsPrinted = 0;
        uint32_t lineSeparatorLen = 1u;
        for (auto i = 0u; i < colsWidth.size(); i++) {
            if (k <= j) {
                if ((lineSeparatorLen + colsWidth[k] < sumGoal + colsWidth.size() - 5) ||
                    (lineSeparatorLen + colsWidth[k] < sumGoal + colsWidth.size() + 1 &&
                        (colTruncated || k == j))) {
                    lineSeparatorLen += colsWidth[k] + 1;
                    k++;
                    colsPrinted++;
                } else if (!colTruncated) {
                    lineSeparatorLen += 6;
                    colTruncated = true;
                }
            }
            if (j >= k) {
                if ((lineSeparatorLen + colsWidth[j] < sumGoal + colsWidth.size() - 5) ||
                    (lineSeparatorLen + colsWidth[j] < sumGoal + colsWidth.size() + 1 &&
                        (colTruncated || j == k))) {
                    lineSeparatorLen += colsWidth[j] + 1;
                    j--;
                    colsPrinted++;
                } else if (!colTruncated) {
                    lineSeparatorLen += 6;
                    colTruncated = true;
                }
            }
        }

        lineSeparator = std::string(lineSeparatorLen, '-');
        printf("%s\n", lineSeparator.c_str());

        if (queryResult.getNumColumns() != 0 && !queryResult.getColumnNames()[0].empty()) {
            std::string printString = "";
            for (auto i = 0; i < k; i++) {
                std::string columnName = queryResult.getColumnNames()[i];
                if (columnName.length() > colsWidth[i] - 2) {
                    columnName = columnName.substr(0, colsWidth[i] - 5) + "...";
                }
                printString += "| ";
                printString += columnName;
                printString += std::string(colsWidth[i] - columnName.length() - 1, ' ');
            }
            if (j >= k) {
                printString += "| ... ";
            }
            for (auto i = j + 1; i < (int)colsWidth.size(); i++) {
                std::string columnName = queryResult.getColumnNames()[i];
                if (columnName.length() > colsWidth[i] - 2) {
                    columnName = columnName.substr(0, colsWidth[i] - 5) + "...";
                }
                printString += "| ";
                printString += columnName;
                printString += std::string(colsWidth[i] - columnName.length() - 1, ' ');
            }
            printf("%s|\n", printString.c_str());
            printf("%s\n", lineSeparator.c_str());
        }

        queryResult.resetIterator();
        rowCount = 0;
        bool rowTruncated = false;
        while (queryResult.hasNext()) {
            if (numTuples > maxRowSize && rowCount >= (maxRowSize / 2) + (maxRowSize % 2 != 0) &&
                rowCount < numTuples - maxRowSize / 2) {
                auto tuple = queryResult.getNext();
                if (!rowTruncated) {
                    rowTruncated = true;
                    uint32_t spacesToPrint = (lineSeparatorLen / 2) - 1;
                    for (auto i = 0u; i < 3; i++) {
                        std::string printString = "|";
                        printString += std::string(spacesToPrint, ' ');
                        printString += ".";
                        if (lineSeparatorLen % 2 == 1) {
                            printString += " ";
                        }
                        printString += std::string(spacesToPrint - 1, ' ');
                        printf("%s|\n", printString.c_str());
                    }
                    printf("%s\n", lineSeparator.c_str());
                }
                rowCount++;
                continue;
            }
            auto tuple = queryResult.getNext();
            auto result = tuple->toString(colsWidth, "|", maxWidth);
            uint64_t startPos = 0;
            std::vector<std::string> colResults;
            for (auto i = 0u; i < colsWidth.size(); i++) {
                uint32_t chrIter = startPos;
                uint32_t fieldLen = 0;
                while (fieldLen < colsWidth[i]) {
                    fieldLen += Utf8Proc::renderWidth(result.c_str(), chrIter);
                    chrIter = utf8proc_next_grapheme(result.c_str(), result.length(), chrIter);
                }
                colResults.push_back(result.substr(startPos, chrIter - startPos));
                // new start position is after the | seperating results
                startPos = chrIter + 1;
            }
            std::string printString = "|";
            for (auto i = 0; i < k; i++) {
                printString += colResults[i] + "|";
            }
            if (j >= k) {
                printString += " ... |";
            }
            for (auto i = j + 1; i < (int)colResults.size(); i++) {
                printString += colResults[i] + "|";
            }
            printf("%s\n", printString.c_str());
            printf("%s\n", lineSeparator.c_str());
            rowCount++;
        }

        // print query result (numFlatTuples & tuples)
        if (numTuples == 1) {
            printf("(1 tuple)\n");
        } else {
            printf("(%" PRIu64 " tuples", numTuples);
            if (rowTruncated) {
                printf(", %" PRIu64 " shown", maxRowSize);
            }
            printf(")\n");
        }
        if (colsWidth.size() == 1) {
            printf("(1 column)\n");
        } else {
            printf("(%" PRIu64 " columns", (uint64_t)colsWidth.size());
            if (colTruncated) {
                printf(", %" PRIu64 " shown", colsPrinted);
            }
            printf(")\n");
        }
        printf("Time: %.2fms (compiling), %.2fms (executing)\n", querySummary->getCompilingTime(),
            querySummary->getExecutionTime());
    }
}

} // namespace main
} // namespace kuzu
