#include "src/main/include/session.h"

namespace graphflow {
namespace main {

Session::Session(const System& system) : system{system} {
    sessionContext = make_unique<SessionContext>();
}

// NOTE: At present, we only support auto-commit transactions.
nlohmann::json Session::executeQuery() {
    auto json = nlohmann::json();
    json["txn"] = beginTransaction();
    // pass transaction around in the following
    if (!json["txn"]["begin"]) {
        return json;
    }
    auto success = true;
    unique_ptr<QueryResult> result;
    try {
        result = system.executeQuery(*sessionContext);
        json["profile"] = sessionContext->profiler.toJson();
        json["result"]["numTuples"] = result->numTuples;
    } catch (exception& e) {
        // query failed to execute
        success = false;
        json["result"] = {};
        json["result"]["error"] = "Query failed: " + string(e.what());
    }
    if (!success) {
        // rollback the transaction if execution fails.
        json["txn"]["rollback"] = rollbackTransaction();
    } else {
        // commit the transaction if the query execution is successful and autoCommit is true.
        json["txn"]["commit"] = commitTransaction();
    }
    return json;
}

nlohmann::json Session::debugInfo() {
    auto json = nlohmann::json();
    json["system"] = *system.debugInfo();
    return json;
}

nlohmann::json Session::beginTransaction() {
    if (sessionContext->activeTransaction) {
        throw invalid_argument("Cannot start nested transaction.");
    }
    auto json = nlohmann::json();
    try {
        sessionContext->activeTransaction = system.transactionManager->startTransaction();
        json["begin"] = true;
        json["id"] = sessionContext->activeTransaction->getId();
    } catch (exception& e) {
        json["begin"] = false;
        json["error"] = "Failed to begin transaction: " + string(e.what());
    }
    return json;
}

nlohmann::json Session::commitTransaction() {
    if (!sessionContext->activeTransaction) {
        throw invalid_argument("No active transaction to commit.");
    }
    auto status = system.transactionManager->commit(sessionContext->activeTransaction);
    auto json = nlohmann::json();
    json["status"] = COMMITTED == status.statusType ? "Committed" : "Rollbacked";
    json["msg"] = status.msg;
    sessionContext->activeTransaction = nullptr;
    return json;
}

nlohmann::json Session::rollbackTransaction() {
    if (!sessionContext->activeTransaction) {
        throw invalid_argument("No active transaction to rollback.");
    }
    auto status = system.transactionManager->rollback(sessionContext->activeTransaction);
    auto json = nlohmann::json();
    json["status"] = "Rollbacked";
    sessionContext->activeTransaction = nullptr;
    return json;
}

} // namespace main
} // namespace graphflow
