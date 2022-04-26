#pragma once

#include "client_context.h"
#include "prepared_statement.h"
#include "query_result.h"

#include "src/planner/logical_plan/include/logical_plan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace main {

class Database;
class Connection {

public:
    explicit Connection(Database* database);

    inline void setMaxNumThreadForExec(uint64_t numThreads) {
        clientContext->numThreadsForExecution = numThreads;
    }

    std::unique_ptr<QueryResult> query(const std::string& query);

    std::unique_ptr<PreparedStatement> prepare(const std::string& query);

    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, pair<string, Args>... args) {
        unordered_map<string, shared_ptr<Literal>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }

    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& inputParams);

    /**
     * TODO(Xiyang): APIs that need to be added
     * * catalog related
     */

    // Used in test helper. Note: for our testing framework, we should not catch exception and
    // instead let IDE catch these exception
    std::vector<unique_ptr<planner::LogicalPlan>> enumeratePlans(const std::string& query);
    std::unique_ptr<QueryResult> executePlan(unique_ptr<planner::LogicalPlan> logicalPlan);
    // Used in API test
    inline uint64_t getMaxNumThreadForExec() const { return clientContext->numThreadsForExecution; }

private:
    std::unique_lock<mutex> acquireLock() { return std::unique_lock<std::mutex>{mtx}; }

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& params, pair<string, T> arg,
        pair<string, Args>... args) {
        auto name = arg.first;
        auto val = make_shared<Literal>(Literal::createLiteral<T>(arg.second));
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParameters(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& inputParams);

    std::unique_ptr<QueryResult> execute(PreparedStatement* preparedStatement);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::mutex mtx;
};

} // namespace main
} // namespace graphflow
