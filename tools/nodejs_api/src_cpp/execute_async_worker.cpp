#include "include/execute_async_worker.h"

#include <chrono>
#include <thread>

#include "include/node_query_result.h"

ExecuteAsyncWorker::ExecuteAsyncWorker(Function& callback, shared_ptr<kuzu::main::Connection>& connection,
    std::string query, NodeQueryResult * nodeQueryResult)
    : AsyncWorker(callback), connection(connection), query(query), nodeQueryResult(nodeQueryResult) {};

void ExecuteAsyncWorker::Execute() {
    try {
        shared_ptr<kuzu::main::QueryResult> queryResult = connection->query(query);
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
