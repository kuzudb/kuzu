#include "EachAsyncWorker.h"
#include <chrono>
#include <thread>
#include "node_query_result.h"
#include <vector>

using namespace std;

EachAsyncWorker::EachAsyncWorker(Function& callback, shared_ptr<kuzu::main::QueryResult>& queryResult)
    : AsyncWorker(callback), queryResult(queryResult) {};

void EachAsyncWorker::Execute() {
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

void EachAsyncWorker::OnOK() {
    for (unsigned int i=0; i < allResult.size(); i++) {
        Napi::Array rowArray = Napi::Array::New(Env(), allResult[i].size() + 1);
        unsigned int j = 0;
        for (; j < allResult[i].size(); j++){
            rowArray.Set(j, allResult[i][j]);
        }
        rowArray.Set(j, allResult.size()-i-1);
        Callback().Call({Env().Null(), rowArray});
    }
};
