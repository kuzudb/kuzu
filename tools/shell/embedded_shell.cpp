#include "tools/shell/include/embedded_shell.h"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <regex>
#include <sstream>

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

const string keywordColorPrefix = "\033[32m\033[1m";
const string keywordResetPostfix = "\033[00m";

const regex specialChars{R"([-[\]{}()*+?.,\^$|#\s])"};

const uint32_t PLAN_INDENT = 3u;
const uint32_t PLAN_MAX_COL_WIDTH = 60u;

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
    string word = "";
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

void EmbeddedShell::initialize() {
    linenoiseHistoryLoad(HISTORY_PATH);
    linenoiseSetCompletionCallback(completion);
    linenoiseSetHighlightCallback(highlight);
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

string genTableFrame(uint32_t len) {
    ostringstream tableFrame;
    for (auto i = 0u; i < len + 2 * PLAN_INDENT; i++) {
        tableFrame << "─";
    }
    return tableFrame.str();
}

void EmbeddedShell::prettyPrintPlanTitle(uint32_t& planBoxWidth) {
    uint32_t numLeftSpaces;
    uint32_t numRightSpaces;
    ostringstream title;
    const string physicalPlan = "Physcial Plan";
    title << "┌" << genTableFrame(planBoxWidth) << "┐" << endl;
    title << "│┌" << genTableFrame(planBoxWidth - 2) << "┐│" << endl;
    numLeftSpaces = (planBoxWidth - physicalPlan.length()) / 2;
    numRightSpaces = planBoxWidth - physicalPlan.length() - numLeftSpaces;
    title << "││" << string(PLAN_INDENT + numLeftSpaces - 1, ' ') << physicalPlan;
    title << string(PLAN_INDENT + numRightSpaces - 1, ' ') << "││" << endl;
    title << "│└" << genTableFrame(planBoxWidth - 2) << "┘│" << endl;
    title << "└" << genTableFrame(planBoxWidth) << "┘" << endl;
    printf("%s", title.str().c_str());
}

bool sortAttributes(string& s1, string& s2) {
    if (regex_search(s1, regex("^name:"))) {
        return true;
    } else if (regex_search(s2, regex("^name:"))) {
        return false;
    } else {
        return s1.compare(s2) < 0;
    }
}

void EmbeddedShell::prettyPrintOperator(
    nlohmann::json& plan, ostringstream& prettyPlan, uint32_t& planBoxWidth) {
    vector<string> attributes;
    ostringstream prettyOperator;
    for (const auto& attribute : plan.items()) {
        if (attribute.key() != "prev") {
            const string& key = attribute.key();
            const string value = regex_replace(attribute.value().dump(), regex("(^\")|(\"$)"), "");
            uint32_t keyValueLength = key.length() + value.length() + 2;
            planBoxWidth = keyValueLength > planBoxWidth ? keyValueLength : planBoxWidth;
            attributes.emplace_back(key + ": " + value);
        }
    }
    sort(attributes.begin(), attributes.end(), sortAttributes);
    planBoxWidth = planBoxWidth > PLAN_MAX_COL_WIDTH ? PLAN_MAX_COL_WIDTH : planBoxWidth;
    if (plan.contains("prev")) {
        prettyPrintOperator(plan["prev"], prettyPlan, planBoxWidth);
    }
    string lineSeparator =
        "│" + string(PLAN_INDENT, ' ') + string(planBoxWidth, '-') + string(PLAN_INDENT, ' ') + "│";
    string boxConnector = string((planBoxWidth + 2 * PLAN_INDENT + 2) / 2, ' ') + "│";
    string tableFrame = genTableFrame(planBoxWidth);
    prettyOperator << "┌" + tableFrame + "┐" << endl;
    for (auto it = attributes.begin(); it != attributes.end(); ++it) {
        string attribute = regex_replace((*it), regex("^name: "), "");
        vector<string> attributeSegments;
        while (attribute.length() > planBoxWidth) {
            attributeSegments.emplace_back(attribute.substr(0, planBoxWidth));
            attribute = attribute.substr(planBoxWidth);
        }
        attributeSegments.emplace_back(attribute);
        for (auto it = attributeSegments.begin(); it != attributeSegments.end(); ++it) {
            prettyOperator << "│" + string(PLAN_INDENT, ' ');
            if ((next(it, 1) == attributeSegments.end()) && (attributeSegments.size() > 1)) {
                prettyOperator << left << setw(planBoxWidth) << setfill(' ') << *it;
            } else {
                string leftSpaces = string((planBoxWidth - it->length()) / 2, ' ');
                string rightSpaces = string(planBoxWidth - it->length() - leftSpaces.length(), ' ');
                prettyOperator << leftSpaces << *it << rightSpaces;
            }
            prettyOperator << string(PLAN_INDENT, ' ') + "│" << endl;
        }
        if (next(it, 1) != attributes.end()) {
            prettyOperator << lineSeparator << endl;
        }
    }
    prettyOperator << "└" + tableFrame + "┘" << endl;
    if (!prettyPlan.str().empty()) {
        prettyOperator << boxConnector << endl;
    }
    prettyPlan.str(prettyOperator.str() + prettyPlan.str());
}

void EmbeddedShell::prettyPrintPlan() {
    ostringstream prettyPlan;
    uint32_t planBoxWidth = 0;
    nlohmann::json js = context.planPrinter->printPlanToJson(*context.profiler);
    prettyPrintOperator(js, prettyPlan, planBoxWidth);
    prettyPrintPlanTitle(planBoxWidth);
    printf("%s", prettyPlan.str().c_str());
}

void EmbeddedShell::printExecutionResult() {
    if (!context.queryResult) { // empty query result
        return;
    }
    if (context.enable_explain) {
        prettyPrintPlan();
    } else {
        // print query result (numFlatTuples & tuples)
        printf(">> Number of output tuples: %lu\n", context.queryResult->getTotalNumFlatTuples());
        printf(">> Compiling time: %.2fms\n", context.compilingTime);
        printf(">> Executing time: %.2fms\n", context.executingTime);

        if (context.queryResult->getNumTuples()) {
            vector<uint32_t> colsWidth(context.queryResult->getTupleSchema().columns.size(), 2);
            uint32_t lineSeparatorLen = 1u + colsWidth.size();
            string lineSeparator;

            auto flatTupleIteartor = context.queryResult->getFlatTuples();
            //  first loop: calculate column width of the table
            while (flatTupleIteartor.hasNextFlatTuple()) {
                auto tuple = flatTupleIteartor.getNextFlatTuple();
                for (auto i = 0u; i < colsWidth.size(); i++) {
                    if (tuple.nullMask[i]) {
                        continue;
                    }
                    uint32_t fieldLen = tuple.getValue(i)->toString().length() + 2;
                    colsWidth[i] = (fieldLen > colsWidth[i]) ? fieldLen : colsWidth[i];
                }
            }
            for (auto width : colsWidth) {
                lineSeparatorLen += width;
            }
            lineSeparator = string(lineSeparatorLen, '-');
            printf("%s\n", lineSeparator.c_str());

            auto flatTupleIteartor1 = context.queryResult->getFlatTuples();
            while (flatTupleIteartor1.hasNextFlatTuple()) {
                auto tuple = flatTupleIteartor1.getNextFlatTuple();
                printf("|%s|\n", tuple.toString(colsWidth, "|").c_str());
                printf("%s\n", lineSeparator.c_str());
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
            prettyPrintPlan();
        }
    }
}
