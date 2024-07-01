#pragma once

#include "linenoise.h"
#include "main/kuzu.h"

namespace kuzu {
namespace main {

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    EmbeddedShell(std::shared_ptr<Database> database, std::shared_ptr<Connection> conn,
        const char* pathToHistory);

    void run();

    static void interruptHandler(int signal);

private:
    int processShellCommands(std::string lineStr);

    static void printHelp();

    void printExecutionResult(QueryResult& queryResult) const;

    void updateTableNames();

    void setMaxRows(const std::string& maxRowsString);

    void setMaxWidth(const std::string& maxWidthString);

private:
    std::shared_ptr<Database> database;
    std::shared_ptr<Connection> conn;
    const char* path_to_history;
    uint64_t maxRowSize;
    uint32_t maxPrintWidth;
};

} // namespace main
} // namespace kuzu
