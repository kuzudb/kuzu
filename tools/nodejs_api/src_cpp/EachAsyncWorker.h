#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class EachAsyncWorker : public AsyncWorker {

public:
    EachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult);
    virtual ~EachAsyncWorker() {};

    void Execute();
    void OnOK();

private:
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<std::vector<std::string>> allResult;
};
