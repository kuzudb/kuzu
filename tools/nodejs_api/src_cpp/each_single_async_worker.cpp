#include "include/each_single_async_worker.h"

#include <chrono>
#include <thread>
#include <vector>

#include "include/node_query_result.h"
#include "include/util.h"

using namespace std;

EachSingleAsyncWorker::EachSingleAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult, size_t index)
    : AsyncWorker(callback), queryResult(queryResult), index(index) {};

void EachSingleAsyncWorker::Execute() {
    try {
        auto row = queryResult->getNext();
        for (size_t j = 0; j < row->len(); j++) {
            rowResult.emplace_back(
                make_unique<kuzu::common::Value>(kuzu::common::Value(*row->getValue(j))));
        }
    } catch(const std::exception &exc) {
        SetError("Unsuccessful async Each callback: " + std::string(exc.what()));
    }
}

void EachSingleAsyncWorker::OnOK() {
    Napi::Array rowArray = Napi::Array::New(Env(), rowResult.size() + 1);
    size_t j = 0;
    for (; j < rowResult.size(); j++){
        rowArray.Set(j, Util::ConvertToNapiObject(*rowResult[j], Env()));
    }
    rowArray.Set(j, Napi::Number::New(Env(), index));
    Callback().Call({Env().Null(), rowArray});
}
