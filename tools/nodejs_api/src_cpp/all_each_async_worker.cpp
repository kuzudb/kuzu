#include "all_each_async_worker.h"

#include <chrono>
#include <thread>
#include <vector>

#include "node_query_result.h"

using namespace std;

AllEachAsyncWorker::AllEachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult,
    std::string executionType)
    : AsyncWorker(callback), queryResult(queryResult), executionType(executionType) {};

void AllEachAsyncWorker::Execute() {
    unsigned int i = 0;
    while(this->queryResult->hasNext()) {
        auto row = this->queryResult->getNext();
        allResult.emplace_back(vector<string>(row->len()));
        for (int j = 0; j < row->len(); j++) {
            allResult[i][j] = row->getValue(j)->toString();
        }
        i++;
    }
};

void AllEachAsyncWorker::OnOK() {
    Napi::Array arr = Napi::Array::New(Env(), allResult.size());
    for (unsigned int i=0; i < allResult.size(); i++) {
        Napi::Array rowArray = Napi::Array::New(Env(), allResult[i].size());
        unsigned int j = 0;
        for (; j < allResult[i].size(); j++){
            rowArray.Set(j, allResult[i][j]);
        }
        if (executionType == "each") {
            rowArray.Set(j, allResult.size()-i-1);
            Callback().Call({Env().Null(), rowArray});
        } else if (executionType == "all") {
            arr.Set(i, rowArray);
        }
    }
    if (executionType == "all") {
        Callback().Call({Env().Null(), arr});
    }
};
