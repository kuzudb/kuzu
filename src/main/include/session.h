#pragma once

#include "src/main/include/system.h"

using namespace std;

namespace graphflow {
namespace main {

// Holds the state of a single connection to the system.
class Session {

public:
    explicit Session(const System& system);

    nlohmann::json executeQuery();

    nlohmann::json debugInfo();

private:
    nlohmann::json beginTransaction();
    nlohmann::json commitTransaction();
    nlohmann::json rollbackTransaction();

public:
    const System& system;
    unique_ptr<SessionContext> sessionContext;
};

} // namespace main
} // namespace graphflow
