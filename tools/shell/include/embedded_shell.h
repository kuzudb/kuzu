#pragma once

#include "linenoise.h"
#include "main/kuzu.h"
#include "output.h"

namespace kuzu {
namespace main {

const int defaultMaxRows = 20;

struct ShellConfig {
    const char* path_to_history = "";
    uint64_t maxRowSize = defaultMaxRows;
    uint32_t maxPrintWidth = 0;
    std::unique_ptr<DrawingCharacters> drawingCharacters = std::make_unique<BoxDrawingCharacters>();
    bool stats = true;
};

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    EmbeddedShell(std::shared_ptr<Database> database, std::shared_ptr<Connection> conn,
        ShellConfig& shellConfig);

    void run();

    std::vector<std::unique_ptr<QueryResult>> processInput(std::string input);

    void printErrorMessage(std::string input, QueryResult& queryResult);

    static void interruptHandler(int signal);

private:
    int processShellCommands(std::string lineStr);

    static void printHelp();

    void printExecutionResult(QueryResult& queryResult) const;

    void printTruncatedExecutionResult(QueryResult& queryResult) const;

    std::string printJsonExecutionResult(QueryResult& queryResult) const;

    std::string printHtmlExecutionResult(QueryResult& queryResult) const;

    std::string printLatexExecutionResult(QueryResult& queryResult) const;

    std::string printLineExecutionResult(QueryResult& queryResult) const;

    void updateTableNames();

    void updateFunctionNames();

    void setMaxRows(const std::string& maxRowsString);

    void setMaxWidth(const std::string& maxWidthString);

    void setMode(const std::string& modeString);

    void setStats(const std::string& statsString);

    void setLinenoiseMode(int mode);

    void setHighlighting(const std::string& highlightingString);

    void setErrors(const std::string& errorsString);

    void setComplete(const std::string& completeString);

private:
    std::shared_ptr<Database> database;
    std::shared_ptr<Connection> conn;
    const char* path_to_history;
    uint64_t maxRowSize;
    uint32_t maxPrintWidth;
    std::unique_ptr<DrawingCharacters> drawingCharacters;
    bool stats;
};

} // namespace main
} // namespace kuzu
