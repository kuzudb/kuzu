#pragma once

#include "linenoise.h"
#include "main/kuzu.h"
#include "processor/processor.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include <json.hpp>

namespace kuzu {
namespace main {

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    EmbeddedShell(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    void run();

private:
    void setNumThreads(const string& numThreadsString);

    void setBufferManagerSize(const string& bufferManagerSizeString);

    void printNodeSchema(const string& tableName);
    void printRelSchema(const string& tableName);

    static void printHelp();

    void printExecutionResult(QueryResult& queryResult) const;

    void updateTableNames();

    void setLoggingLevel(const string& loggingLevel);

private:
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
};

} // namespace main
} // namespace kuzu
