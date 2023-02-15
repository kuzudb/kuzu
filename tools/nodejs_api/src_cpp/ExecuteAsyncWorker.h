#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class ExecuteAsyncWorker : public AsyncWorker {

public:
    ExecuteAsyncWorker(Function& callback, shared_ptr<kuzu::main::Connection>& connection,
        std::string query);
    virtual ~ExecuteAsyncWorker() {};

    void Execute();
    void OnOK();

private:
    std::string query;
    unique_ptr<kuzu::main::QueryResult> queryResult;
    shared_ptr<kuzu::main::Connection> connection;
};
