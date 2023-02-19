#include "execute_async_worker.h"

#include <chrono>
#include <thread>

#include "node_query_result.h"

ExecuteAsyncWorker::ExecuteAsyncWorker(Function& callback, shared_ptr<kuzu::main::Connection>& connection,
    std::string query)
    : AsyncWorker(callback), connection(connection), query(query) {};

void ExecuteAsyncWorker::Execute() {
    this->queryResult = connection->query(query);
    if (!queryResult->isSuccess()) {
      SetError("Unsuccessful execute: " + queryResult->getErrorMessage());
    }
};

void ExecuteAsyncWorker::OnOK() {
    shared_ptr<kuzu::main::QueryResult> queryResult = std::move(this->queryResult);
    Callback().Call({Env().Null(), NodeQueryResult::Wrap(Env(), queryResult)});
};
