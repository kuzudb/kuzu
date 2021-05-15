#include "src/main/include/session.h"

namespace graphflow {
namespace main {

unique_ptr<nlohmann::json> Session::debugInfo() {
    auto json = make_unique<nlohmann::json>();
    (*json)["system"] = *system->debugInfo();
    return json;
}

// NOTE: At present, we only support auto-commit transactions.
unique_ptr<nlohmann::json> Session::submitQuery(const string query, const uint32_t numThreads) {
    if (!system->isInitialized()) {
        throw invalid_argument("The system is not properly initialized");
    }
    auto json = make_unique<nlohmann::json>();
    (*json)["txn"] = *beginTransaction();
    if (!(*json)["txn"]["begin"]) {
        return json;
    }
    auto success = true;
    unique_ptr<QueryResult> result;
    try {
        auto allPlans = system->enumerateLogicalPlans(query);
        result = system->execute(move(allPlans[0]), activeTransaction, numThreads);
        (*json)["result"]["numTuples"] = result->numTuples;
        (*json)["result"]["duration"] = result->duration.count();
    } catch (exception& e) {
        // query failed to execute
        success = false;
        (*json)["result"] = {};
        (*json)["result"]["error"] = "Query failed: " + string(e.what());
    }
    if (!success) {
        // rollback the transaction if execution fails.
        (*json)["txn"]["rollback"] = *rollbackTransaction();
    } else {
        // commit the transaction if the query execution is successful and autoCommit is true.
        (*json)["txn"]["commit"] = *commitTransaction();
    }
    return json;
}

unique_ptr<nlohmann::json> Session::beginTransaction() {
    if (activeTransaction) {
        throw invalid_argument("Cannot start nested transaction.");
    }
    auto json = make_unique<nlohmann::json>();
    try {
        activeTransaction = system->transactionManager->startTranscation();
        (*json)["begin"] = true;
        (*json)["id"] = activeTransaction->getId();
    } catch (exception& e) {
        (*json)["begin"] = false;
        (*json)["error"] = "Failed to begin transaction: " + string(e.what());
    }
    return json;
}

unique_ptr<nlohmann::json> Session::commitTransaction() {
    if (!activeTransaction) {
        throw invalid_argument("No active transaction to commit.");
    }
    auto status = system->transactionManager->commit(activeTransaction);
    auto json = make_unique<nlohmann::json>();
    (*json)["status"] = COMMITTED == status.statusType ? "Committed" : "Rollbacked";
    (*json)["msg"] = status.msg;
    activeTransaction = nullptr;
    return json;
}

unique_ptr<nlohmann::json> Session::rollbackTransaction() {
    if (!activeTransaction) {
        throw invalid_argument("No active transaction to rollback.");
    }
    auto status = system->transactionManager->rollback(activeTransaction);
    auto json = make_unique<nlohmann::json>();
    (*json)["status"] = "Rollbacked";
    activeTransaction = nullptr;
    return json;
}

} // namespace main
} // namespace graphflow
