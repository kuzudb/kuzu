#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class EachAsyncWorker : public AsyncWorker {

public:
    EachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult, size_t index);
    virtual ~EachAsyncWorker() {};

    void Execute();
    void OnOK();
    Napi::Value ConvertToNapiObject(const kuzu::common::Value& value,  Napi::Env env);

private:
    size_t index;
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<unique_ptr<kuzu::common::Value>> rowResult;
};
