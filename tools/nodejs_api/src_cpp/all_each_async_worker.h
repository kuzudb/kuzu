#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class AllEachAsyncWorker : public AsyncWorker {

public:
    AllEachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult, std::string executionType);
    AllEachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult, std::string executionType, Function& eachCallback);
    virtual ~AllEachAsyncWorker() {};

    void Execute();
    void OnOK();
    Napi::Value ConvertToNapiObject(const kuzu::common::Value& value,  Napi::Env env);

private:
    Function& eachCallback;
    std::string executionType;
    shared_ptr<kuzu::main::QueryResult> queryResult;
    std::vector<std::vector<unique_ptr<kuzu::common::Value>>> allResult;
};
