#include "include/all_async_worker.h"

#include <chrono>
#include <thread>
#include <vector>

#include "include/node_query_result.h"
#include "include/util.h"

using namespace std;

AllAsyncWorker::AllAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult)
    : AsyncWorker(callback), queryResult(queryResult) {};

void AllAsyncWorker::Execute() {
    try {
        size_t i = 0;
        while (this->queryResult->hasNext()) {
            auto row = this->queryResult->getNext();
            allResult.emplace_back(vector<unique_ptr<kuzu::common::Value>>(row->len()));
            for (size_t j = 0; j < row->len(); j++) {
                allResult[i][j] =
                    make_unique<kuzu::common::Value>(kuzu::common::Value(*row->getValue(j)));
            }
            i++;
        }
    } catch(const std::exception &exc) {
        SetError("Unsuccessful async All/Each callback: " + std::string(exc.what()));
    }
};

void AllAsyncWorker::OnOK() {
    Napi::Array arr = Napi::Array::New(Env(), allResult.size());
    for (size_t i=0; i < allResult.size(); i++) {
        Napi::Array rowArray = Napi::Array::New(Env(), allResult[i].size());
        size_t j = 0;
        for (; j < allResult[i].size(); j++){
            rowArray.Set(j, Util::ConvertToNapiObject(*allResult[i][j], Env()));
        }
        arr.Set(i, rowArray);
    }
    Callback().Call({Env().Null(), arr});
};
