#pragma once

#include "tools/shell/include/linenoise.h"

#include "src/main/include/system.h"

using namespace graphflow::main;

/**
 * Embedded shell simulate a session that directly connects to the system.
 */
class EmbeddedShell {

public:
    explicit EmbeddedShell(const System& system) : system{system}, context{SessionContext()} {};

    void initialize();

    void run();

private:
    void printHelp();

    void printExecutionResult();

private:
    const System& system;
    SessionContext context;
};
