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
#include <iomanip>
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
const char* CONPROMPT = "\u00B7 ";
const char* SCONPROMPT = "\u2023 ";

// build-in shell command
struct ShellCommand {
    const char* HELP = ":help";
    const char* CLEAR = ":clear";
    const char* QUIT = ":quit";
    const char* MAX_ROWS = ":max_rows";
    const char* MAX_WIDTH = ":max_width";
    const char* MODE = ":mode";
    const char* STATS = ":stats";
    const char* MULTI = ":multiline";
    const char* SINGLE = ":singleline";
    const char* HIGHLIGHT = ":highlight";
    const char* ERRORS = ":render_errors";
    const char* COMPLETE = ":render_completion";
    const std::array<const char*, 12> commandList = {HELP, CLEAR, QUIT, MAX_ROWS, MAX_WIDTH, MODE,
        STATS, MULTI, SINGLE, HIGHLIGHT, ERRORS, COMPLETE};
} shellCommand;

const char* TAB = "    ";

const std::array<const char*, _keywordListLength> keywordList = _keywordList;

const char* keywordColorPrefix = "\033[32m\033[1m";
const char* keywordResetPostfix = "\033[39m\033[22m";

// NOLINTNEXTLINE(cert-err58-cpp): OK to have a global regex, even if the constructor allocates.
const std::regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

std::vector<std::string> nodeTableNames;
std::vector<std::string> relTableNames;
std::unordered_map<std::string, std::vector<std::string>> tableColumnNames;

std::vector<std::string> functionNames;
std::vector<std::string> tableFunctionNames;

bool continueLine = false;
std::string currLine;
std::string historyLine;

static Connection* globalConnection;

bool printInterrupted = false;

#ifndef _WIN32
struct termios orig_termios;
bool noEcho = false;
#else
DWORD oldOutputCP;
#endif

void EmbeddedShell::updateTableNames() {
    nodeTableNames.clear();
    relTableNames.clear();
    for (auto& tableEntry : database->catalog->getTableEntries(&transaction::DUMMY_TRANSACTION)) {
        if (tableEntry->getType() == catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
            nodeTableNames.push_back(tableEntry->getName());
        } else if (tableEntry->getType() == catalog::CatalogEntryType::REL_TABLE_ENTRY) {
            relTableNames.push_back(tableEntry->getName());
        } else {
            continue;
        }
        std::vector<std::string> columnNames;
        for (auto& column : tableEntry->getProperties()) {
            columnNames.push_back(column.getName());
        }
        tableColumnNames[tableEntry->getName()] = columnNames;
    }
}

void EmbeddedShell::updateFunctionNames() {
    functionNames.clear();
    auto function = database->catalog->getFunctions(&transaction::DUMMY_TRANSACTION);
    for (auto& [_, entry] : function->getEntries(&transaction::DUMMY_TRANSACTION)) {
        if (entry->getType() == catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY) {
            tableFunctionNames.push_back(entry->getName());
        } else {
            functionNames.push_back(entry->getName());
        }
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

bool isInsideCommentOrQuote(const std::string& buffer) {
    bool insideSingleLineComment = false;
    bool insideMultiLineComment = false;
    bool insideDoubleQuote = false;
    bool insideSingleQuote = false;

    for (size_t i = 0; i < buffer.size(); ++i) {
        if (insideSingleLineComment) {
            if (buffer[i] == '\n') {
                insideSingleLineComment = false;
            }
        } else if (insideMultiLineComment) {
            if (buffer[i] == '*' && i + 1 < buffer.size() && buffer[i + 1] == '/') {
                insideMultiLineComment = false;
                ++i;
            }
        } else if (insideDoubleQuote) {
            if (buffer[i] == '"' && (i == 0 || buffer[i - 1] != '\\')) {
                insideDoubleQuote = false;
            }
        } else if (insideSingleQuote) {
            if (buffer[i] == '\'' && (i == 0 || buffer[i - 1] != '\\')) {
                insideSingleQuote = false;
            }
        } else {
            if (buffer[i] == '/' && i + 1 < buffer.size()) {
                if (buffer[i + 1] == '/') {
                    insideSingleLineComment = true;
                    ++i;
                } else if (buffer[i + 1] == '*') {
                    insideMultiLineComment = true;
                    ++i;
                }
            } else if (buffer[i] == '"') {
                insideDoubleQuote = true;
            } else if (buffer[i] == '\'') {
                insideSingleQuote = true;
            }
        }
    }

    return insideSingleLineComment || insideMultiLineComment || insideDoubleQuote ||
           insideSingleQuote;
}

void findTableVariableNames(const std::string buf, std::regex tableRegex,
    std::vector<std::string>& tempTableNames, std::vector<std::string>& foundTableNames) {
    auto matches_begin = std::sregex_iterator(buf.begin(), buf.end(), tableRegex);
    auto matches_end = std::sregex_iterator();

    for (std::sregex_iterator i = matches_begin; i != matches_end; ++i) {
        std::smatch match = *i;
        tempTableNames.push_back(match[1].str());
        foundTableNames.push_back(match[2].str());
    }
}

void keywordCompletion(std::string buf, std::string prefix, std::string keyword,
    linenoiseCompletions* lc) {
    std::string bufEscaped = regex_replace(buf, specialChars, R"(\$&)");
    if (regex_search(keyword, std::regex("^" + bufEscaped, std::regex_constants::icase))) {
        std::string transformedKeyword = keyword;
        for (size_t i = 0; i < buf.size() && i < keyword.size(); ++i) {
            if (islower(buf[i])) {
                transformedKeyword[i] = tolower(keyword[i]);
            } else if (isupper(buf[i])) {
                transformedKeyword[i] = toupper(keyword[i]);
            }
        }
        // make the rest of the keyword the same case as the last character of buf
        auto caseTransform = islower(buf.back()) ? ::tolower : ::toupper;
        std::transform(keyword.begin() + buf.size(), keyword.end(),
            transformedKeyword.begin() + buf.size(), caseTransform);

        linenoiseAddCompletion(lc, (prefix + transformedKeyword).c_str());
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

    // Skip completion if inside a comment or quote
    if (isInsideCommentOrQuote(buf)) {
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

    std::vector<std::string> tempTableNames;
    std::vector<std::string> foundTableNames;
    for (auto& node : nodeTableNames) {
        std::regex nodeTableRegex("\\(([^:]+):(" + node + ")\\)");
        findTableVariableNames(buf, nodeTableRegex, tempTableNames, foundTableNames);
    }
    for (auto& rel : relTableNames) {
        std::regex relTableRegex("\\[([^:]+):(" + rel + ")\\]");
        findTableVariableNames(buf, relTableRegex, tempTableNames, foundTableNames);
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

    for (size_t i = 0; i < tempTableNames.size(); ++i) {
        if (regex_search(buf, std::regex("^" + tempTableNames[i] + R"(\.[^.]*)"))) {
            auto it = tableColumnNames.find(foundTableNames[i]);
            if (it != tableColumnNames.end()) {
                auto columnNames = it->second;
                for (auto& columnName : columnNames) {
                    if (regex_search(columnName,
                            std::regex("^" + buf.substr(tempTableNames[i].size() + 1)))) {
                        linenoiseAddCompletion(lc,
                            (prefix + tempTableNames[i] + "." + columnName).c_str());
                    }
                }
            }
            return;
        }
    }

    if (regex_search(prefix, std::regex("^\\s*CALL\\s*$", std::regex_constants::icase))) {
        for (auto& function : tableFunctionNames) {
            keywordCompletion(buf, prefix, function, lc);
        }
        return;
    }

    for (std::string keyword : keywordList) {
        keywordCompletion(buf, prefix, keyword, lc);
    }

    for (std::string function : functionNames) {
        keywordCompletion(buf, prefix, function, lc);
    }
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
    std::istringstream iss(lineStr);
    std::string command, arg;
    iss >> command >> arg;
    if (command == shellCommand.HELP) {
        printHelp();
    } else if (command == shellCommand.CLEAR) {
        linenoiseClearScreen();
    } else if (command == shellCommand.QUIT) {
        return -1;
    } else if (command == shellCommand.MAX_ROWS) {
        setMaxRows(arg);
    } else if (command == shellCommand.MAX_WIDTH) {
        setMaxWidth(arg);
    } else if (command == shellCommand.MODE) {
        setMode(arg);
    } else if (command == shellCommand.STATS) {
        setStats(arg);
    } else if (command == shellCommand.MULTI) {
        setLinenoiseMode(1);
    } else if (command == shellCommand.SINGLE) {
        setLinenoiseMode(0);
    } else if (command == shellCommand.HIGHLIGHT) {
        setHighlighting(arg);
    } else if (command == shellCommand.ERRORS) {
        setErrors(arg);
    } else if (command == shellCommand.COMPLETE) {
        setComplete(arg);
    } else {
        printf("Error: Unknown command: \"%s\". Enter \":help\" for help\n", lineStr.c_str());
        printf("Did you mean: \"%s\"?\n", findClosestCommand(lineStr).c_str());
    }
    return 0;
}

EmbeddedShell::EmbeddedShell(std::shared_ptr<Database> database, std::shared_ptr<Connection> conn,
    ShellConfig& shellConfig) {
    path_to_history = shellConfig.path_to_history;
    linenoiseHistoryLoad(path_to_history);
    linenoiseSetCompletionCallback(completion);
    this->database = database;
    this->conn = conn;
    globalConnection = conn.get();
    maxRowSize = shellConfig.maxRowSize;
    maxPrintWidth = shellConfig.maxPrintWidth;
    drawingCharacters = std::move(shellConfig.drawingCharacters);
    stats = shellConfig.stats;
    updateTableNames();
    updateFunctionNames();
    auto sigResult = std::signal(SIGINT, interruptHandler);
    if (sigResult == SIG_ERR) {
        throw std::runtime_error("Error: Failed to set signal handler");
    }
}

std::vector<std::unique_ptr<QueryResult>> EmbeddedShell::processInput(std::string input) {
    std::string query;
    std::stringstream ss;
    std::vector<std::unique_ptr<QueryResult>> queryResults;
    historyLine = "";
    // Append rest of multiline query to current input line
    if (continueLine) {
        input = std::move(currLine) + std::move(input);
        currLine = "";
        continueLine = false;
    }
    input = input.erase(input.find_last_not_of(" \t\n\r\f\v") + 1);
    // process shell commands
    if (!continueLine && input[0] == ':') {
        processShellCommands(input);
        // process queries
    } else if (!input.empty() && cypherComplete((char*)input.c_str())) {
        ss.clear();
        ss.str(input);
        while (getline(ss, query, ';')) {
            queryResults.push_back(conn->query(query));
        }
        // set up multiline query if current query doesn't end with a semicolon
    } else if (!input.empty() && input[0] != ':') {
        continueLine = true;
        currLine += input + "\n";
    }
    updateTableNames();
    historyLine = input;
    return queryResults;
}

void EmbeddedShell::printErrorMessage(std::string input, QueryResult& queryResult) {
    input = input.erase(input.find_last_not_of(" \t\n\r\f\v") + 1);
    std::string errMsg = queryResult.getErrorMessage();
    printf("Error: %s\n", errMsg.c_str());
    if (errMsg.find(ParserException::ERROR_PREFIX) == 0) {
        std::string trimmedinput = input;
        trimmedinput.erase(0, trimmedinput.find_first_not_of(" \t\n\r\f\v"));
        if (trimmedinput.find_first_of(" \t\n\r\f\v") == std::string::npos) {
            printf("\"%s\" is not a valid Cypher query. Did you mean to issue a "
                   "CLI command, e.g., \"%s\"?\n",
                input.c_str(), findClosestCommand(input).c_str());
        }
    }
}

void EmbeddedShell::run() {
    char* line = nullptr;
    const char ctrl_c = '\3';
    int numCtrlC = 0;
    continueLine = false;
    currLine = "";

#ifndef _WIN32
    termios raw{};
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

    while (
        (line = linenoise(continueLine ? ALTPROMPT : PROMPT, CONPROMPT, SCONPROMPT)) != nullptr) {
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
        auto queryResults = processInput(lineStr);
        if (queryResults.empty()) {
            std::istringstream iss(lineStr);
            std::string command;
            iss >> command;
            if (command == shellCommand.QUIT) {
                free(line);
                break;
            }
        }
        for (auto& queryResult : queryResults) {
            if (queryResult->isSuccess()) {
                printInterrupted = false;
                printExecutionResult(*queryResult);
            } else {
                printErrorMessage(lineStr, *queryResult);
            }
        }
        if (!continueLine && !historyLine.empty()) {
            linenoiseHistoryAdd(historyLine.c_str());
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
    printInterrupted = true;
}

void EmbeddedShell::setLinenoiseMode(int mode) {
    linenoiseSetMultiLine(mode, path_to_history);
    if (mode == 0) {
        printf("Single line mode enabled\n");
    } else {
        printf("Multi line mode enabled\n");
    }
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

void printModeInfo() {
    printf("Available output modes:\n");
    printf("%sbox (default):%sTables using unicode box-drawing characters\n", TAB, TAB);
    printf("%scolumn:%sOutput in columns\n", TAB, TAB);
    printf("%scsv:%sComma-separated values\n", TAB, TAB);
    printf("%shtml:%sHTML table\n", TAB, TAB);
    printf("%sjson:%sResults in a JSON array\n", TAB, TAB);
    printf("%sjsonlines:%sResults in a NDJSON format\n", TAB, TAB);
    printf("%slatex:%sLaTeX tabular environment code\n", TAB, TAB);
    printf("%sline:%sOne value per line\n", TAB, TAB);
    printf("%slist:%sValues delimited by \"|\"\n", TAB, TAB);
    printf("%smarkdown:%sMarkdown table\n", TAB, TAB);
    printf("%stable:%sTables using ASCII characters\n", TAB, TAB);
    printf("%stsv:%sTab-separated values\n", TAB, TAB);
    printf("%strash:%sNo output\n", TAB, TAB);
}

void EmbeddedShell::setMode(const std::string& modeString) {
    if (modeString.empty()) {
        printModeInfo();
        return;
    }
    switch (PrintTypeUtils::fromString(modeString)) {
    case PrintType::BOX:
        drawingCharacters = std::make_unique<BoxDrawingCharacters>();
        break;
    case PrintType::TABLE:
        drawingCharacters = std::make_unique<TableDrawingCharacters>();
        break;
    case PrintType::CSV:
        drawingCharacters = std::make_unique<CSVDrawingCharacters>();
        break;
    case PrintType::TSV:
        drawingCharacters = std::make_unique<TSVDrawingCharacters>();
        break;
    case PrintType::MARKDOWN:
        drawingCharacters = std::make_unique<MarkdownDrawingCharacters>();
        break;
    case PrintType::COLUMN:
        drawingCharacters = std::make_unique<ColumnDrawingCharacters>();
        break;
    case PrintType::LIST:
        drawingCharacters = std::make_unique<ListDrawingCharacters>();
        break;
    case PrintType::TRASH:
        drawingCharacters = std::make_unique<TrashDrawingCharacters>();
        break;
    case PrintType::JSON:
        drawingCharacters = std::make_unique<JSONDrawingCharacters>();
        break;
    case PrintType::JSONLINES:
        drawingCharacters = std::make_unique<JSONLinesDrawingCharacters>();
        break;
    case PrintType::HTML:
        drawingCharacters = std::make_unique<HTMLDrawingCharacters>();
        break;
    case PrintType::LATEX:
        drawingCharacters = std::make_unique<LatexDrawingCharacters>();
        break;
    case PrintType::LINE:
        drawingCharacters = std::make_unique<LineDrawingCharacters>();
        break;
    default:
        printf("Cannot parse '%s' as output mode.\n\n", modeString.c_str());
        printModeInfo();
        return;
    }
    printf("mode set as %s\n", modeString.c_str());
}

void EmbeddedShell::setStats(const std::string& statsString) {
    std::string statsStringLower = statsString;
    std::transform(statsStringLower.begin(), statsStringLower.end(), statsStringLower.begin(),
        [](unsigned char c) { return std::tolower(c); });
    if (statsStringLower == "on") {
        stats = true;
    } else if (statsStringLower == "off") {
        stats = false;
    } else {
        printf("Cannot parse '%s' to toggle stats. Expect 'on' or 'off'.\n", statsString.c_str());
        return;
    }
    printf("stats set as %s\n", stats ? "on" : "off");
}

void EmbeddedShell::setHighlighting(const std::string& highlightString) {
    std::string highlightStringLower = highlightString;
    std::transform(highlightStringLower.begin(), highlightStringLower.end(),
        highlightStringLower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (highlightStringLower == "on") {
        linenoiseSetHighlighting(1);
        printf("enabled syntax highlighting\n");
    } else if (highlightStringLower == "off") {
        linenoiseSetHighlighting(0);
        printf("disabled syntax highlighting\n");
    } else {
        printf("Cannot parse '%s' to toggle highlighting. Expect 'on' or 'off'.\n",
            highlightString.c_str());
        return;
    }
}

void EmbeddedShell::setErrors(const std::string& errorsString) {
    std::string errorsStringLower = errorsString;
    std::transform(errorsStringLower.begin(), errorsStringLower.end(), errorsStringLower.begin(),
        [](unsigned char c) { return std::tolower(c); });
    if (errorsStringLower == "on") {
        linenoiseSetErrors(1);
        printf("enabled error highlighting\n");
    } else if (errorsStringLower == "off") {
        linenoiseSetErrors(0);
        printf("disabled error highlighting\n");
    } else {
        printf("Cannot parse '%s' to toggle error highlighting. Expect 'on' or 'off'.\n",
            errorsStringLower.c_str());
        return;
    }
}

void EmbeddedShell::setComplete(const std::string& completeString) {
    std::string completeStringLower = completeString;
    std::transform(completeStringLower.begin(), completeStringLower.end(),
        completeStringLower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (completeStringLower == "on") {
        linenoiseSetCompletion(1);
        printf("enabled completion highlighting\n");
    } else if (completeStringLower == "off") {
        linenoiseSetCompletion(0);
        printf("disabled completion highlighting\n");
    } else {
        printf("Cannot parse '%s' to toggle completion highlighting. Expect 'on' or 'off'.\n",
            completeStringLower.c_str());
        return;
    }
}

void EmbeddedShell::printHelp() {
    printf("%s%s %sget command list\n", TAB, shellCommand.HELP, TAB);
    printf("%s%s %sclear shell\n", TAB, shellCommand.CLEAR, TAB);
    printf("%s%s %sexit from shell\n", TAB, shellCommand.QUIT, TAB);
    printf("%s%s [max_rows] %sset maximum number of rows for display (default: 20)\n", TAB,
        shellCommand.MAX_ROWS, TAB);
    printf("%s%s [max_width] %sset maximum width in characters for display\n", TAB,
        shellCommand.MAX_WIDTH, TAB);
    printf("%s%s [mode] %sset output mode (default: box)\n", TAB, shellCommand.MODE, TAB);
    printf("%s%s [on|off] %stoggle query stats on or off\n", TAB, shellCommand.STATS, TAB);
    printf("%s%s %sset multiline mode (default)\n", TAB, shellCommand.MULTI, TAB);
    printf("%s%s %sset singleline mode\n", TAB, shellCommand.SINGLE, TAB);
    printf("%s%s [on|off] %stoggle syntax highlighting on or off\n", TAB, shellCommand.HIGHLIGHT,
        TAB);
    printf("%s%s [on|off] %stoggle error highlighting on or off\n", TAB, shellCommand.ERRORS, TAB);
    printf("%s%s [on|off] %stoggle completion highlighting on or off\n", TAB, shellCommand.COMPLETE,
        TAB);
    printf("\n");
    printf("%sNote: you can change and see several system configurations, such as num-threads, \n",
        TAB);
    printf("%s%s  timeout, and progress_bar using Cypher CALL statements.\n", TAB, TAB);
    printf("%s%s  e.g. CALL THREADS=5; or CALL current_setting('threads') return *;\n", TAB, TAB);
    const char* url = "https://docs.kuzudb.com/cypher/configuration";
    printf("%s%s  See: \x1B]8;;%s\x1B\\%s\x1B]8;;\x1B\\\n", TAB, TAB, url, url);
}

std::string escapeJsonString(const std::string& str) {
    std::ostringstream escaped;
    for (char c : str) {
        switch (c) {
        case '"':
            escaped << "\\\"";
            break;
        case '\\':
            escaped << "\\\\";
            break;
        case '\b':
            escaped << "\\b";
            break;
        case '\f':
            escaped << "\\f";
            break;
        case '\n':
            escaped << "\\n";
            break;
        case '\r':
            escaped << "\\r";
            break;
        case '\t':
            escaped << "\\t";
            break;
        default:
            if ('\x00' <= c && c <= '\x1f') {
                escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
            } else {
                escaped << c;
            }
        }
    }
    return escaped.str();
}

std::string EmbeddedShell::printJsonExecutionResult(QueryResult& queryResult) const {
    auto colNames = queryResult.getColumnNames();
    auto jsonDrawingCharacters =
        common::ku_dynamic_cast<JSONDrawingCharacters*>(drawingCharacters.get());
    bool jsonLines = jsonDrawingCharacters->printType == PrintType::JSONLINES;
    std::string printString = "";
    if (!jsonLines) {
        printString = jsonDrawingCharacters->ArrayOpen;
    }
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        printString += jsonDrawingCharacters->ObjectOpen;
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += jsonDrawingCharacters->KeyValue;
            printString += escapeJsonString(colNames[i]);
            printString += jsonDrawingCharacters->KeyValue;
            printString += jsonDrawingCharacters->KeyDelimiter;
            printString += jsonDrawingCharacters->KeyValue;
            printString += escapeJsonString(tuple->getValue(i)->toString());
            printString += jsonDrawingCharacters->KeyValue;
            if (i != queryResult.getNumColumns() - 1) {
                printString += jsonDrawingCharacters->TupleDelimiter;
            }
        }
        printString += jsonDrawingCharacters->ObjectClose;
        if (queryResult.hasNext()) {
            if (!jsonLines) {
                printString += jsonDrawingCharacters->TupleDelimiter;
            }
            printString += "\n";
        }
    }
    if (!jsonLines) {
        printString += jsonDrawingCharacters->ArrayClose;
    }
    printString += "\n";
    return printString;
}

std::string escapeHtmlString(const std::string& str) {
    std::ostringstream escaped;
    for (char c : str) {
        switch (c) {
        case '&':
            escaped << "&amp;";
            break;
        case '\"':
            escaped << "&quot;";
            break;
        case '\'':
            escaped << "&apos;";
            break;
        case '<':
            escaped << "&lt;";
            break;
        case '>':
            escaped << "&gt;";
            break;
        default:
            escaped << c;
        }
    }
    return escaped.str();
}

std::string EmbeddedShell::printHtmlExecutionResult(QueryResult& queryResult) const {
    auto colNames = queryResult.getColumnNames();
    auto htmlDrawingCharacters =
        common::ku_dynamic_cast<HTMLDrawingCharacters*>(drawingCharacters.get());
    std::string printString = htmlDrawingCharacters->TableOpen;
    printString += "\n";
    printString += htmlDrawingCharacters->RowOpen;
    printString += "\n";
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += htmlDrawingCharacters->HeaderOpen;
        printString += escapeHtmlString(colNames[i]);
        printString += htmlDrawingCharacters->HeaderClose;
    }
    printString += "\n";
    printString += htmlDrawingCharacters->RowClose;
    printString += "\n";
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        printString += htmlDrawingCharacters->RowOpen;
        printString += "\n";
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += htmlDrawingCharacters->CellOpen;
            printString += escapeHtmlString(tuple->getValue(i)->toString());
            printString += htmlDrawingCharacters->CellClose;
        }
        printString += htmlDrawingCharacters->RowClose;
        printString += "\n";
    }
    printString += htmlDrawingCharacters->TableClose;
    printString += "\n";
    return printString;
}

std::string escapeLatexString(const std::string& str) {
    std::ostringstream escaped;
    for (char c : str) {
        switch (c) {
        case '&':
            escaped << "\\&";
            break;
        case '%':
            escaped << "\\%";
            break;
        case '$':
            escaped << "\\$";
            break;
        case '#':
            escaped << "\\#";
            break;
        case '_':
            escaped << "\\_";
            break;
        case '{':
            escaped << "\\{";
            break;
        case '}':
            escaped << "\\}";
            break;
        case '~':
            escaped << "\\textasciitilde{}";
            break;
        case '^':
            escaped << "\\textasciicircum{}";
            break;
        case '\\':
            escaped << "\\textbackslash{}";
            break;
        case '<':
            escaped << "\\textless{}";
            break;
        case '>':
            escaped << "\\textgreater{}";
            break;
        default:
            escaped << c;
        }
    }
    return escaped.str();
}

std::string EmbeddedShell::printLatexExecutionResult(QueryResult& queryResult) const {
    auto colNames = queryResult.getColumnNames();
    auto latexDrawingCharacters =
        common::ku_dynamic_cast<LatexDrawingCharacters*>(drawingCharacters.get());
    std::string printString = latexDrawingCharacters->TableOpen;
    printString += latexDrawingCharacters->AlignOpen;
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += latexDrawingCharacters->ColumnAlign;
    }
    printString += latexDrawingCharacters->AlignClose;
    printString += "\n";
    printString += latexDrawingCharacters->Line;
    printString += "\n";
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += escapeLatexString(colNames[i]);
        if (i != queryResult.getNumColumns() - 1) {
            printString += latexDrawingCharacters->TupleDelimiter;
        }
    }
    printString += latexDrawingCharacters->EndLine;
    printString += "\n";
    printString += latexDrawingCharacters->Line;
    printString += "\n";
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += escapeLatexString(tuple->getValue(i)->toString());
            if (i != queryResult.getNumColumns() - 1) {
                printString += latexDrawingCharacters->TupleDelimiter;
            }
        }
        printString += latexDrawingCharacters->EndLine;
        printString += "\n";
    }
    printString += latexDrawingCharacters->Line;
    printString += "\n";
    printString += latexDrawingCharacters->TableClose;
    printString += "\n";
    return printString;
}

std::string EmbeddedShell::printLineExecutionResult(QueryResult& queryResult) const {
    auto colNames = queryResult.getColumnNames();
    std::string printString = "";
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += colNames[i];
            printString += drawingCharacters->TupleDelimiter;
            printString += tuple->getValue(i)->toString();
            printString += "\n";
        }
        printString += "\n";
    }
    return printString;
}

std::string escapeCsvString(const std::string& field, const std::string& delimiter) {
    bool needsQuoting =
        field.find_first_of("\"" + delimiter + "\n") != std::string::npos || field.empty();
    if (!needsQuoting) {
        return field;
    }

    std::ostringstream quotedField;
    quotedField << '"';
    for (char c : field) {
        if (c == '"') {
            quotedField << "\"\"";
        } else {
            quotedField << c;
        }
    }
    quotedField << '"';
    return quotedField.str();
}

void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->isExplain()) {
        printf("%s", queryResult.getNext()->toString().c_str());
        return;
    }
    if (PrintTypeUtils::isTableType(drawingCharacters->printType)) {
        printTruncatedExecutionResult(queryResult);
        return;
    }
    std::string printString = "";
    if (drawingCharacters->printType == PrintType::JSON ||
        drawingCharacters->printType == PrintType::JSONLINES) {
        printString = printJsonExecutionResult(queryResult);
    } else if (drawingCharacters->printType == PrintType::HTML) {
        printString = printHtmlExecutionResult(queryResult);
    } else if (drawingCharacters->printType == PrintType::LATEX) {
        printString = printLatexExecutionResult(queryResult);
    } else if (drawingCharacters->printType == PrintType::LINE) {
        printString = printLineExecutionResult(queryResult);
    } else if (drawingCharacters->printType != PrintType::TRASH) {
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString +=
                escapeCsvString(queryResult.getColumnNames()[i], drawingCharacters->TupleDelimiter);
            if (i != queryResult.getNumColumns() - 1) {
                printString += drawingCharacters->TupleDelimiter;
            }
        }
        printString += "\n";
        while (queryResult.hasNext()) {
            if (printInterrupted) {
                return;
            }
            auto tuple = queryResult.getNext();
            for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
                std::string field = tuple->getValue(i)->toString();
                printString += escapeCsvString(field, drawingCharacters->TupleDelimiter);
                if (i != queryResult.getNumColumns() - 1) {
                    printString += drawingCharacters->TupleDelimiter;
                }
            }
            printString += "\n";
        }
    }
    if (!printInterrupted) {
        printf("%s", printString.c_str());
    }
    if (stats && !printInterrupted) {
        if (queryResult.getNumTuples() == 1) {
            printf("(1 tuple)\n");
        } else {
            printf("(%" PRIu64 " tuples)\n", queryResult.getNumTuples());
        }
        if (queryResult.getNumColumns() == 1) {
            printf("(1 column)\n");
        } else {
            printf("(%" PRIu64 " columns)\n", static_cast<uint64_t>(queryResult.getNumColumns()));
        }
        printf("Time: %.2fms (compiling), %.2fms (executing)\n", querySummary->getCompilingTime(),
            querySummary->getExecutionTime());
    }
}

void EmbeddedShell::printTruncatedExecutionResult(QueryResult& queryResult) const {
    auto tableDrawingCharacters =
        common::ku_dynamic_cast<BaseTableDrawingCharacters*>(drawingCharacters.get());
    auto querySummary = queryResult.getQuerySummary();
    constexpr uint32_t SMALL_TABLE_SEPERATOR_LENGTH = 3;
    const uint32_t minTruncatedWidth = 20;
    uint64_t numTuples = queryResult.getNumTuples();
    std::vector<uint32_t> colsWidth(queryResult.getNumColumns(), 2);
    // calculate the width of each column name/type
    for (auto i = 0u; i < colsWidth.size(); i++) {
        colsWidth[i] = std::max(queryResult.getColumnNames()[i].length(),
                           queryResult.getColumnDataTypes()[i].toString().length()) +
                       2;
    }
    uint64_t rowCount = 0;
    // calculate the width of each tuple value
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

    // calculate the maximum width of the table
    uint32_t sumGoal = minTruncatedWidth;
    uint32_t maxWidth = minTruncatedWidth;
    if (colsWidth.size() == 1) {
        uint32_t minDisplayWidth = minTruncatedWidth + SMALL_TABLE_SEPERATOR_LENGTH;
        if (maxPrintWidth > minDisplayWidth) {
            sumGoal = maxPrintWidth - 2;
        } else {
            sumGoal =
                std::max((uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
                    minDisplayWidth);
        }
    } else if (colsWidth.size() > 1) {
        uint32_t minDisplayWidth = SMALL_TABLE_SEPERATOR_LENGTH + minTruncatedWidth * 2;
        if (maxPrintWidth > minDisplayWidth) {
            sumGoal = maxPrintWidth - colsWidth.size() - 1;
        } else {
            // make sure there is space for the first and last column
            sumGoal =
                std::max((uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
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

    // truncate columns
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
            uint32_t sumDifference = sum - ((oldValue - secondHighestValue) * maxValueIndex.size());
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

    // remove columns
    uint32_t k = 0;
    uint32_t j = colsWidth.size() - 1;
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

    if (queryResult.getNumColumns() != 0 && tableDrawingCharacters->TopLine) {
        std::string printString;
        printString += tableDrawingCharacters->DownAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->DownAndHorizontal;
            }
        }
        if (j >= k) {
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->DownAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->DownAndHorizontal;
            }
        }
        printString += tableDrawingCharacters->DownAndLeft;
        printf("%s\n", printString.c_str());
    }

    if (queryResult.getNumColumns() != 0 && !queryResult.getColumnNames()[0].empty()) {
        std::string printString;
        printString += tableDrawingCharacters->Vertical;
        for (auto i = 0u; i < k; i++) {
            std::string columnName = queryResult.getColumnNames()[i];
            uint32_t chrIter = 0;
            uint32_t fieldLen = 0;
            while (chrIter < columnName.length()) {
                fieldLen += Utf8Proc::renderWidth(columnName.c_str(), chrIter);
                chrIter = utf8proc_next_grapheme(columnName.c_str(), columnName.length(), chrIter);
            }
            uint32_t truncationLen = fieldLen;
            if (truncationLen > colsWidth[i] - 2) {
                while (truncationLen > colsWidth[i] - 5) {
                    chrIter = Utf8Proc::previousGraphemeCluster(columnName.c_str(),
                        columnName.length(), chrIter);
                    truncationLen -= Utf8Proc::renderWidth(columnName.c_str(), chrIter);
                }
                columnName = columnName.substr(0, chrIter) + tableDrawingCharacters->Truncation;
                truncationLen += strlen(tableDrawingCharacters->Truncation);
            }
            printString += " ";
            printString += columnName;
            printString += std::string(colsWidth[i] - truncationLen - 1, ' ');
            if (i != k - 1) {
                printString += tableDrawingCharacters->Vertical;
            }
        }
        if (j >= k) {
            printString += tableDrawingCharacters->Vertical;
            printString += " ";
            printString += tableDrawingCharacters->Truncation;
            printString += " ";
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            printString += tableDrawingCharacters->Vertical;
            std::string columnName = queryResult.getColumnNames()[i];
            uint32_t chrIter = 0;
            uint32_t fieldLen = 0;
            while (chrIter < columnName.length()) {
                fieldLen += Utf8Proc::renderWidth(columnName.c_str(), chrIter);
                chrIter = utf8proc_next_grapheme(columnName.c_str(), columnName.length(), chrIter);
            }
            uint32_t truncationLen = fieldLen;
            if (truncationLen > colsWidth[i] - 2) {
                while (truncationLen > colsWidth[i] - 5) {
                    chrIter = Utf8Proc::previousGraphemeCluster(columnName.c_str(),
                        columnName.length(), chrIter);
                    truncationLen -= Utf8Proc::renderWidth(columnName.c_str(), chrIter);
                }
                columnName = columnName.substr(0, chrIter) + tableDrawingCharacters->Truncation;
                truncationLen += strlen(tableDrawingCharacters->Truncation);
            }
            printString += " ";
            printString += columnName;
            printString += std::string(colsWidth[i] - truncationLen - 1, ' ');
        }
        printString += tableDrawingCharacters->Vertical;
        if (tableDrawingCharacters->Types) {
            printString += "\n";
            printString += tableDrawingCharacters->Vertical;
            for (auto i = 0u; i < k; i++) {
                std::string columnType = queryResult.getColumnDataTypes()[i].toString();
                if (columnType.length() > colsWidth[i] - 2) {
                    columnType =
                        columnType.substr(0, colsWidth[i] - 5) + tableDrawingCharacters->Truncation;
                }
                printString += " ";
                printString += columnType;
                printString += std::string(colsWidth[i] - columnType.length() - 1, ' ');
                if (i != k - 1) {
                    printString += tableDrawingCharacters->Vertical;
                }
            }
            if (j >= k) {
                printString += tableDrawingCharacters->Vertical;
                printString += "     ";
            }
            for (auto i = j + 1; i < colsWidth.size(); i++) {
                std::string columnType = queryResult.getColumnDataTypes()[i].toString();
                if (columnType.length() > colsWidth[i] - 2) {
                    columnType =
                        columnType.substr(0, colsWidth[i] - 5) + tableDrawingCharacters->Truncation;
                }
                printString += tableDrawingCharacters->Vertical;
                printString += " ";
                printString += columnType;
                printString += std::string(colsWidth[i] - columnType.length() - 1, ' ');
            }
            printString += tableDrawingCharacters->Vertical;
        }
        printf("%s\n", printString.c_str());
    }

    if (queryResult.getNumColumns() != 0) {
        std::string printString;
        printString += tableDrawingCharacters->VerticalAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->VerticalAndHorizontal;
            }
        }
        if (j >= k) {
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->VerticalAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->VerticalAndHorizontal;
            }
        }
        printString += tableDrawingCharacters->VerticalAndLeft;
        printf("%s\n", printString.c_str());
    }

    queryResult.resetIterator();
    rowCount = 0;
    bool rowTruncated = false;
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return;
        }
        if (numTuples > maxRowSize && rowCount >= (maxRowSize / 2) + (maxRowSize % 2 != 0) &&
            rowCount < numTuples - maxRowSize / 2) {
            auto tuple = queryResult.getNext();
            if (!rowTruncated) {
                rowTruncated = true;
                for (auto i = 0u; i < 3u; i++) {
                    std::string printString;
                    printString += tableDrawingCharacters->Vertical;
                    for (auto l = 0u; l < k; l++) {
                        uint32_t spacesToPrint = (colsWidth[l] / 2);
                        printString += std::string(spacesToPrint - 1, ' ');
                        printString += tableDrawingCharacters->MiddleDot;
                        if (colsWidth[l] % 2 == 1) {
                            printString += " ";
                        }
                        printString += std::string(spacesToPrint, ' ');
                        if (l != k - 1) {
                            printString += tableDrawingCharacters->Vertical;
                        }
                    }
                    if (j >= k) {
                        printString += tableDrawingCharacters->Vertical;
                        printString += "  ";
                        printString += tableDrawingCharacters->MiddleDot;
                        printString += "  ";
                    }
                    for (auto l = j + 1; l < colsWidth.size(); l++) {
                        uint32_t spacesToPrint = (colsWidth[l] / 2);
                        printString += tableDrawingCharacters->Vertical;
                        printString += std::string(spacesToPrint - 1, ' ');
                        printString += tableDrawingCharacters->MiddleDot;
                        if (colsWidth[l] % 2 == 1) {
                            printString += " ";
                        }
                        printString += std::string(spacesToPrint, ' ');
                    }
                    printString += tableDrawingCharacters->Vertical;
                    printf("%s\n", printString.c_str());
                }
            }
            rowCount++;
            continue;
        }
        auto tuple = queryResult.getNext();
        auto result = tuple->toString(colsWidth, tableDrawingCharacters->TupleDelimiter, maxWidth);
        std::string printString;
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
        printString += tableDrawingCharacters->Vertical;
        for (auto i = 0u; i < k; i++) {
            printString += colResults[i];
            if (i != k - 1) {
                printString += tableDrawingCharacters->Vertical;
            }
        }
        if (j >= k) {
            printString += tableDrawingCharacters->Vertical;
            printString += " ";
            printString += tableDrawingCharacters->Truncation;
            printString += " ";
        }
        for (auto i = j + 1; i < colResults.size(); i++) {
            printString += tableDrawingCharacters->Vertical;
            printString += colResults[i];
        }
        printString += tableDrawingCharacters->Vertical;
        printf("%s\n", printString.c_str());
        rowCount++;
    }

    if (queryResult.getNumColumns() != 0 && tableDrawingCharacters->BottomLine) {
        std::string printString;
        printString += tableDrawingCharacters->UpAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->UpAndHorizontal;
            }
        }
        if (j >= k) {
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->Horizontal;
            printString += tableDrawingCharacters->UpAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += tableDrawingCharacters->Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += tableDrawingCharacters->UpAndHorizontal;
            }
        }
        printString += tableDrawingCharacters->UpAndLeft;
        printf("%s\n", printString.c_str());
    }

    // print query result (numFlatTuples & tuples)
    if (stats) {
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
