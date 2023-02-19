#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class AllEachAsyncWorker : public AsyncWorker {

public:
    AllEachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult, std::string executionType);
    virtual ~AllEachAsyncWorker() {};

    void Execute();
    void OnOK();

private:
    std::string executionType;
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<std::vector<std::string>> allResult;
};
