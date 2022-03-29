#pragma once

#include "tools/shell/include/linenoise.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::main;

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    EmbeddedShell(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    void run();

private:
    static void printHelp();

    void printExecutionResult(QueryResult& queryResult) const;

private:
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
};
