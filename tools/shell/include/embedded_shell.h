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
    EmbeddedShell(const std::string& databasePath, const SystemConfig& systemConfig);

    void run();

private:
    void setNumThreads(const std::string& numThreadsString);

    void setBufferManagerSize(const std::string& bufferManagerSizeString);

    void printNodeSchema(const std::string& tableName);
    void printRelSchema(const std::string& tableName);

    static void printHelp();

    void printExecutionResult(QueryResult& queryResult) const;

    void updateTableNames();

    void setLoggingLevel(const std::string& loggingLevel);

private:
    std::unique_ptr<Database> database;
    std::unique_ptr<Connection> conn;
};

} // namespace main
} // namespace kuzu
