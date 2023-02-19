#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class AllAsyncWorker : public AsyncWorker {

public:
    AllAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult);
    virtual ~AllAsyncWorker() {};

    void Execute();
    void OnOK();

private:
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<std::vector<std::string>> allResult;
};
