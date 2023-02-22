#pragma once
#include <napi.h>
#include "main/kuzu.h"

using namespace Napi;

class EachSingleAsyncWorker : public AsyncWorker {

public:
    EachSingleAsyncWorker(Function& callback, shared_ptr<kuzu::common::FlatTuple>& flatTuple, size_t index);
    virtual ~EachSingleAsyncWorker() {};

    void Execute();
    void OnOK();
    Napi::Value ConvertToNapiObject(const kuzu::common::Value& value,  Napi::Env env);

private:
    size_t index;
    shared_ptr<kuzu::common::FlatTuple> flatTuple;
    std::vector<unique_ptr<kuzu::common::Value>> rowResult;
};
