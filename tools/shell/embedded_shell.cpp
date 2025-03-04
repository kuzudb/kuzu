#include "embedded_shell.h"

#include "binder/visitor/confidential_statement_analyzer.h"

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

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/parser.h"
#include "keywords.h"
#include "parser/parser.h"
#include "printer/json_printer.h"
#include "printer/printer_factory.h"
#include "processor/result/factorized_table.h"
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

std::vector<std::string> functionAndTypeNames;
std::vector<std::string> tableFunctionNames;
std::vector<std::string> aggregateFunctionNames;
std::vector<std::string> scalarFunctionNames;

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
    for (auto& tableEntry : database->catalog->getTableEntries(&transaction::DUMMY_TRANSACTION,
             false /*useInternal*/)) {
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

void EmbeddedShell::updateFunctionAndTypeNames() {
    functionAndTypeNames.clear();
    auto typeNames = LogicalTypeUtils::getAllValidLogicTypeIDs();
    for (auto& type : typeNames) {
        functionAndTypeNames.push_back(LogicalTypeUtils::toString(type));
    }
    for (auto& entry : database->catalog->getFunctionEntries(&transaction::DUMMY_TRANSACTION)) {
        switch (entry->getType()) {
        case catalog::CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
            aggregateFunctionNames.push_back(entry->getName());
            break;
        case catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY:
            scalarFunctionNames.push_back(entry->getName());
            break;
        case catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY:
        case catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY:
            tableFunctionNames.push_back(entry->getName());
            break;
        default:
            functionAndTypeNames.push_back(entry->getName());
            break;
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

    for (std::string function : aggregateFunctionNames) {
        keywordCompletion(buf, prefix, function, lc);
    }

    for (std::string function : scalarFunctionNames) {
        keywordCompletion(buf, prefix, function, lc);
    }

    for (std::string function : functionAndTypeNames) {
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
    printer = std::move(shellConfig.printer);
    stats = shellConfig.stats;
    updateTableNames();
    updateFunctionAndTypeNames();
    auto sigResult = std::signal(SIGINT, interruptHandler);
    if (sigResult == SIG_ERR) {
        throw std::runtime_error("Error: Failed to set signal handler");
    }
}

std::string decodeEscapeSequences(const std::string& input) {
    std::regex unicodeRegex(R"(\\u([0-9A-Fa-f]{4})|\\U([0-9A-Fa-f]{8}))");
    std::string result = input;
    std::smatch match;
    while (std::regex_search(result, match, unicodeRegex)) {
        std::string codepointStr;
        if (match[1].matched) {
            codepointStr = match[1].str();
        } else if (match[2].matched) {
            codepointStr = match[2].str();
        }
        uint32_t codepoint = static_cast<uint32_t>(std::stoull(codepointStr, nullptr, 16));
        if (codepoint > 0x10FFFF) {
            throw std::runtime_error("Invalid Unicode codepoint");
        }
        if (codepoint == 0) {
            throw std::runtime_error("Null character not allowed");
        }
        // Check for surrogate pairs
        if (0xD800 <= codepoint && codepoint <= 0xDBFF) {
            // High surrogate, look for the next low surrogate
            std::smatch nextMatch;
            std::string remainingString = result.substr(match.position() + match.length());
            if (std::regex_search(remainingString, nextMatch, unicodeRegex)) {
                std::string nextCodepointStr = nextMatch[1].str();
                int nextCodepoint = std::stoi(nextCodepointStr, nullptr, 16);
                if (0xDC00 <= nextCodepoint && nextCodepoint <= 0xDFFF) {
                    // Valid surrogate pair
                    codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (nextCodepoint - 0xDC00);
                    result.replace(match.position() + match.length(), nextMatch.length(), "");
                } else {
                    throw std::runtime_error("Invalid surrogate pair");
                }
            } else {
                throw std::runtime_error("Unmatched high surrogate");
            }
        }

        // Convert codepoint to UTF-8
        char utf8Char[5] = {0}; // UTF-8 characters can be up to 4 bytes + null terminator
        int size = 0;
        if (!Utf8Proc::codepointToUtf8(codepoint, size, utf8Char)) {
            throw std::runtime_error("Failed to convert codepoint to UTF-8");
        }

        // Replace the escape sequence with the actual UTF-8 character
        result.replace(match.position(), match.length(), std::string(utf8Char, size));
    }
    return result;
}

void EmbeddedShell::checkConfidentialStatement(const std::string& query,
    const QueryResult* queryResult, std::string& input) {
    if (queryResult->isSuccess() && !database->getConfig().readOnly &&
        queryResult->getQuerySummary()->getStatementType() ==
            common::StatementType::STANDALONE_CALL) {
        auto clientContext = conn->getClientContext();
        auto parsedQueries = clientContext->parseQuery(query);
        for (auto& parsedQuery : parsedQueries) {
            clientContext->getTransactionContext()->beginWriteTransaction();
            auto binder = binder::Binder(clientContext);
            auto boundQuery = binder.bind(*parsedQuery);
            auto boundStatementVisitor = binder::ConfidentialStatementAnalyzer{};
            boundStatementVisitor.visit(*boundQuery);
            if (boundStatementVisitor.isConfidential()) {
                input = "";
            }
            clientContext->getTransactionContext()->commit();
        }
    }
}

std::vector<std::unique_ptr<QueryResult>> EmbeddedShell::processInput(std::string input) {
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
    // Decode escape sequences
    std::string unicodeInput;
    try {
        unicodeInput = decodeEscapeSequences(input);
    } catch (std::exception& e) {
        printf("Error: %s\n", e.what());
        historyLine = input;
        return queryResults;
    }
    // process shell commands
    if (!continueLine && unicodeInput[0] == ':') {
        processShellCommands(unicodeInput);
        // process queries
    } else if (!unicodeInput.empty() && cypherComplete((char*)unicodeInput.c_str())) {
        ss.clear();
        ss.str(unicodeInput);
        auto result = conn->query(unicodeInput);
        auto curr = result.get();
        checkConfidentialStatement(unicodeInput, curr, input);
        while (true) {
            queryResults.push_back(std::move(result));
            if (!curr->getNextQueryResult()) {
                break;
            }
            result = std::move(curr->nextQueryResult);
            curr = result.get();
            checkConfidentialStatement(unicodeInput, curr, input);
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
    auto modePrinter = PrinterFactory::getPrinter(PrinterTypeUtils::fromString(modeString));
    if (modePrinter == nullptr) {
        printf("Cannot parse '%s' as output mode.\n\n", modeString.c_str());
        printModeInfo();
        return;
    }
    stats = modePrinter->defaultPrintStats();
    printer = std::move(modePrinter);
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

std::string EmbeddedShell::printJsonExecutionResult(QueryResult& queryResult) const {
    auto& jsonPrinter = printer->constCast<JsonPrinter>();
    return jsonPrinter.print(queryResult, *conn->getClientContext()->getMemoryManager());
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
    auto& pHtmlPrinter = printer->constCast<HTMLPrinter>();
    std::string printString = pHtmlPrinter.TableOpen;
    printString += "\n";
    printString += pHtmlPrinter.RowOpen;
    printString += "\n";
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += pHtmlPrinter.HeaderOpen;
        printString += escapeHtmlString(colNames[i]);
        printString += pHtmlPrinter.HeaderClose;
    }
    printString += "\n";
    printString += pHtmlPrinter.RowClose;
    printString += "\n";
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        printString += pHtmlPrinter.RowOpen;
        printString += "\n";
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += pHtmlPrinter.CellOpen;
            printString += escapeHtmlString(tuple->getValue(i)->toString());
            printString += pHtmlPrinter.CellClose;
        }
        printString += pHtmlPrinter.RowClose;
        printString += "\n";
    }
    printString += pHtmlPrinter.TableClose;
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
    auto& latexPrinter = printer->constCast<LatexPrinter>();
    std::string printString = latexPrinter.TableOpen;
    printString += latexPrinter.AlignOpen;
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += latexPrinter.ColumnAlign;
    }
    printString += latexPrinter.AlignClose;
    printString += "\n";
    printString += latexPrinter.Line;
    printString += "\n";
    for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
        printString += escapeLatexString(colNames[i]);
        if (i != queryResult.getNumColumns() - 1) {
            printString += latexPrinter.TupleDelimiter;
        }
    }
    printString += latexPrinter.EndLine;
    printString += "\n";
    printString += latexPrinter.Line;
    printString += "\n";
    while (queryResult.hasNext()) {
        if (printInterrupted) {
            return std::string();
        }
        auto tuple = queryResult.getNext();
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString += escapeLatexString(tuple->getValue(i)->toString());
            if (i != queryResult.getNumColumns() - 1) {
                printString += latexPrinter.TupleDelimiter;
            }
        }
        printString += latexPrinter.EndLine;
        printString += "\n";
    }
    printString += latexPrinter.Line;
    printString += "\n";
    printString += latexPrinter.TableClose;
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
            printString += printer->TupleDelimiter;
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

// TODO(Ziyi): Move the printing logic under each printer and each printer should expose a unified
// `print` interface.
void EmbeddedShell::printExecutionResult(QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    if (querySummary->isExplain()) {
        printf("%s", queryResult.getNext()->toString().c_str());
        return;
    }
    if (PrinterTypeUtils::isTableType(printer->printType)) {
        printTruncatedExecutionResult(queryResult);
        return;
    }
    std::string printString = "";
    if (printer->printType == PrinterType::JSON || printer->printType == PrinterType::JSONLINES) {
        printString = printJsonExecutionResult(queryResult);
    } else if (printer->printType == PrinterType::HTML) {
        printString = printHtmlExecutionResult(queryResult);
    } else if (printer->printType == PrinterType::LATEX) {
        printString = printLatexExecutionResult(queryResult);
    } else if (printer->printType == PrinterType::LINE) {
        printString = printLineExecutionResult(queryResult);
    } else if (printer->printType != PrinterType::TRASH) {
        for (auto i = 0u; i < queryResult.getNumColumns(); i++) {
            printString +=
                escapeCsvString(queryResult.getColumnNames()[i], printer->TupleDelimiter);
            if (i != queryResult.getNumColumns() - 1) {
                printString += printer->TupleDelimiter;
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
                printString += escapeCsvString(field, printer->TupleDelimiter);
                if (i != queryResult.getNumColumns() - 1) {
                    printString += printer->TupleDelimiter;
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
    auto& baseTablePrinter = printer->constCast<BaseTablePrinter>();
    auto querySummary = queryResult.getQuerySummary();
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
    uint32_t sumGoal = BaseTablePrinter::MIN_TRUNCATED_WIDTH;
    uint32_t maxWidth = BaseTablePrinter::MIN_TRUNCATED_WIDTH;
    ;
    if (colsWidth.size() == 1) {
        uint32_t minDisplayWidth =
            BaseTablePrinter::MIN_TRUNCATED_WIDTH + BaseTablePrinter::SMALL_TABLE_SEPERATOR_LENGTH;
        if (maxPrintWidth > minDisplayWidth) {
            sumGoal = maxPrintWidth - 2;
        } else {
            sumGoal =
                std::max((uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
                    minDisplayWidth);
        }
    } else if (colsWidth.size() > 1) {
        uint32_t minDisplayWidth = BaseTablePrinter::SMALL_TABLE_SEPERATOR_LENGTH +
                                   BaseTablePrinter::MIN_TRUNCATED_WIDTH * 2;
        if (maxPrintWidth > minDisplayWidth) {
            sumGoal = maxPrintWidth - colsWidth.size() - 1;
        } else {
            // make sure there is space for the first and last column
            sumGoal =
                std::max((uint32_t)(getColumns(STDIN_FILENO, STDOUT_FILENO) - colsWidth.size() - 1),
                    minDisplayWidth);
        }
    } else if (maxPrintWidth > BaseTablePrinter::MIN_TRUNCATED_WIDTH) {
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
        if (secondHighestValue < BaseTablePrinter::MIN_TRUNCATED_WIDTH + 2 &&
            newValue < BaseTablePrinter::MIN_TRUNCATED_WIDTH + 2) {
            newValue = BaseTablePrinter::MIN_TRUNCATED_WIDTH + 2;
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
        if (newValue == BaseTablePrinter::MIN_TRUNCATED_WIDTH + 2) {
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

    if (queryResult.getNumColumns() != 0 && baseTablePrinter.TopLine) {
        std::string printString;
        printString += baseTablePrinter.DownAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.DownAndHorizontal;
            }
        }
        if (j >= k) {
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.DownAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.DownAndHorizontal;
            }
        }
        printString += baseTablePrinter.DownAndLeft;
        printf("%s\n", printString.c_str());
    }

    if (queryResult.getNumColumns() != 0 && !queryResult.getColumnNames()[0].empty()) {
        std::string printString;
        printString += baseTablePrinter.Vertical;
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
                columnName = columnName.substr(0, chrIter) + baseTablePrinter.Truncation;
                truncationLen += strlen(baseTablePrinter.Truncation);
            }
            printString += " ";
            printString += columnName;
            printString += std::string(colsWidth[i] - truncationLen - 1, ' ');
            if (i != k - 1) {
                printString += baseTablePrinter.Vertical;
            }
        }
        if (j >= k) {
            printString += baseTablePrinter.Vertical;
            printString += " ";
            printString += baseTablePrinter.Truncation;
            printString += " ";
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            printString += baseTablePrinter.Vertical;
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
                columnName = columnName.substr(0, chrIter) + baseTablePrinter.Truncation;
                truncationLen += strlen(baseTablePrinter.Truncation);
            }
            printString += " ";
            printString += columnName;
            printString += std::string(colsWidth[i] - truncationLen - 1, ' ');
        }
        printString += baseTablePrinter.Vertical;
        if (baseTablePrinter.Types) {
            printString += "\n";
            printString += baseTablePrinter.Vertical;
            for (auto i = 0u; i < k; i++) {
                std::string columnType = queryResult.getColumnDataTypes()[i].toString();
                if (columnType.length() > colsWidth[i] - 2) {
                    columnType =
                        columnType.substr(0, colsWidth[i] - 5) + baseTablePrinter.Truncation;
                }
                printString += " ";
                printString += columnType;
                printString += std::string(colsWidth[i] - columnType.length() - 1, ' ');
                if (i != k - 1) {
                    printString += baseTablePrinter.Vertical;
                }
            }
            if (j >= k) {
                printString += baseTablePrinter.Vertical;
                printString += "     ";
            }
            for (auto i = j + 1; i < colsWidth.size(); i++) {
                std::string columnType = queryResult.getColumnDataTypes()[i].toString();
                if (columnType.length() > colsWidth[i] - 2) {
                    columnType =
                        columnType.substr(0, colsWidth[i] - 5) + baseTablePrinter.Truncation;
                }
                printString += baseTablePrinter.Vertical;
                printString += " ";
                printString += columnType;
                printString += std::string(colsWidth[i] - columnType.length() - 1, ' ');
            }
            printString += baseTablePrinter.Vertical;
        }
        printf("%s\n", printString.c_str());
    }

    if (queryResult.getNumColumns() != 0) {
        std::string printString;
        printString += baseTablePrinter.VerticalAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.VerticalAndHorizontal;
            }
        }
        if (j >= k) {
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.VerticalAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.VerticalAndHorizontal;
            }
        }
        printString += baseTablePrinter.VerticalAndLeft;
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
                    printString += baseTablePrinter.Vertical;
                    for (auto l = 0u; l < k; l++) {
                        uint32_t spacesToPrint = (colsWidth[l] / 2);
                        printString += std::string(spacesToPrint - 1, ' ');
                        printString += baseTablePrinter.MiddleDot;
                        if (colsWidth[l] % 2 == 1) {
                            printString += " ";
                        }
                        printString += std::string(spacesToPrint, ' ');
                        if (l != k - 1) {
                            printString += baseTablePrinter.Vertical;
                        }
                    }
                    if (j >= k) {
                        printString += baseTablePrinter.Vertical;
                        printString += "  ";
                        printString += baseTablePrinter.MiddleDot;
                        printString += "  ";
                    }
                    for (auto l = j + 1; l < colsWidth.size(); l++) {
                        uint32_t spacesToPrint = (colsWidth[l] / 2);
                        printString += baseTablePrinter.Vertical;
                        printString += std::string(spacesToPrint - 1, ' ');
                        printString += baseTablePrinter.MiddleDot;
                        if (colsWidth[l] % 2 == 1) {
                            printString += " ";
                        }
                        printString += std::string(spacesToPrint, ' ');
                    }
                    printString += baseTablePrinter.Vertical;
                    printf("%s\n", printString.c_str());
                }
            }
            rowCount++;
            continue;
        }
        auto tuple = queryResult.getNext();
        auto result = tuple->toString(colsWidth, baseTablePrinter.TupleDelimiter, maxWidth);
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
        printString += baseTablePrinter.Vertical;
        for (auto i = 0u; i < k; i++) {
            printString += colResults[i];
            if (i != k - 1) {
                printString += baseTablePrinter.Vertical;
            }
        }
        if (j >= k) {
            printString += baseTablePrinter.Vertical;
            printString += " ";
            printString += baseTablePrinter.Truncation;
            printString += " ";
        }
        for (auto i = j + 1; i < colResults.size(); i++) {
            printString += baseTablePrinter.Vertical;
            printString += colResults[i];
        }
        printString += baseTablePrinter.Vertical;
        printf("%s\n", printString.c_str());
        rowCount++;
    }

    if (queryResult.getNumColumns() != 0 && baseTablePrinter.BottomLine) {
        std::string printString;
        printString += baseTablePrinter.UpAndRight;
        for (auto i = 0u; i < k; i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.UpAndHorizontal;
            }
        }
        if (j >= k) {
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.Horizontal;
            printString += baseTablePrinter.UpAndHorizontal;
        }
        for (auto i = j + 1; i < colsWidth.size(); i++) {
            for (auto l = 0u; l < colsWidth[i]; l++) {
                printString += baseTablePrinter.Horizontal;
            }
            if (i != colsWidth.size() - 1) {
                printString += baseTablePrinter.UpAndHorizontal;
            }
        }
        printString += baseTablePrinter.UpAndLeft;
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
