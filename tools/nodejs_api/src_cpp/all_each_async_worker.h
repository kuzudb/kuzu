#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class AllEachAsyncWorker : public AsyncWorker {

public:
    AllEachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult);
    virtual ~AllEachAsyncWorker() {};

    void Execute();
    void OnOK();
    Napi::Value ConvertToNapiObject(const kuzu::common::Value& value,  Napi::Env env);

private:
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<std::vector<unique_ptr<kuzu::common::Value>>> allResult;
};
