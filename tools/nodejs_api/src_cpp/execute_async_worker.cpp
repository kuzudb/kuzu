#include "include/execute_async_worker.h"

#include <chrono>
#include <thread>

#include "include/node_query_result.h"

using namespace Napi;

ExecuteAsyncWorker::ExecuteAsyncWorker(Function& callback, shared_ptr<kuzu::main::Connection>& connection,
    std::string query, NodeQueryResult * nodeQueryResult, unordered_map<std::string, shared_ptr<kuzu::common::Value>>& params)
    : AsyncWorker(callback), connection(connection), query(query), nodeQueryResult(nodeQueryResult)
      , params(params) {};

void ExecuteAsyncWorker::Execute() {
    try {
        shared_ptr<kuzu::main::QueryResult> queryResult;
        if (!params.empty()) {
            auto preparedStatement = std::move(connection->prepare(query));
            queryResult = connection->executeWithParams(preparedStatement.get(), params);
        } else {
             queryResult = connection->query(query);
        }

        if (!queryResult->isSuccess()) {
            SetError("Query async execute unsuccessful: " + queryResult->getErrorMessage());
        }
        nodeQueryResult->SetQueryResult(queryResult);
    }
    catch(const std::exception &exc) {
        SetError("Unsuccessful async execute: " + std::string(exc.what()));
    }
};

void ExecuteAsyncWorker::OnOK() {
    Callback().Call({Env().Null()});
};
