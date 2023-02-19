#include "AllAsyncWorker.h"
#include <chrono>
#include <thread>
#include "node_query_result.h"
#include <vector>

using namespace std;

AllAsyncWorker::AllAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult)
    : AsyncWorker(callback), queryResult(queryResult) {};

void AllAsyncWorker::Execute() {
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

void AllAsyncWorker::OnOK() {
    Napi::Array arr = Napi::Array::New(Env(), allResult.size());
    for (unsigned int i=0; i < allResult.size(); i++) {
        Napi::Array rowArray = Napi::Array::New(Env(), allResult[i].size());
        for (int j = 0; j < allResult[i].size(); j++){
            rowArray.Set(j, allResult[i][j]);
        }
        arr.Set(i, rowArray);
    }
    Callback().Call({Env().Null(), arr});
};
