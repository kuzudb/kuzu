#pragma once

#include "thread"

#include "src/main/include/system.h"
#include "src/planner/include/logical_plan/logical_plan.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

// Holds the state of a single connection to the system.
class Session {

public:
    Session(unique_ptr<System>& system) : system{system}, activeTransaction{nullptr} {};

    unique_ptr<nlohmann::json> submitQuery(string query, uint32_t numThreads);

    unique_ptr<nlohmann::json> debugInfo();

private:
    unique_ptr<nlohmann::json> beginTransaction();
    unique_ptr<nlohmann::json> commitTransaction();
    unique_ptr<nlohmann::json> rollbackTransaction();

private:
    unique_ptr<System>& system;
    Transaction* activeTransaction;
};

} // namespace main
} // namespace graphflow
