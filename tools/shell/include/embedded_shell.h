#pragma once

#include "tools/shell/include/linenoise.h"

#include "src/main/include/system.h"

using namespace graphflow::main;

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    EmbeddedShell(const System& system);

    void run();

private:
    static void printHelp();

    void printExecutionResult() const;

    void prettyPrintPlan() const;

private:
    const System& system;
    SessionContext context;
};
