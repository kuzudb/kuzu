#ifndef KUZU_NODE_QUERY_RESULT_H
#define KUZU_NODE_QUERY_RESULT_H

#include <napi.h>
#include <iostream>
#include "main/kuzu.h"
#include "binder/bound_statement_result.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/result/factorized_table.h"

using namespace std;

class NodeQueryResult: public Napi::ObjectWrap<NodeQueryResult> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  void SetQueryResult(shared_ptr<kuzu::main::QueryResult> & inputQueryResult);
  NodeQueryResult(const Napi::CallbackInfo& info);
  ~NodeQueryResult() = default;

 private:
    void Close(const Napi::CallbackInfo& info);
    Napi::Value All(const Napi::CallbackInfo& info);
    Napi::Value Each(const Napi::CallbackInfo& info);
    Napi::Value GetColumnDataTypes(const Napi::CallbackInfo& info);
    Napi::Value GetColumnNames(const Napi::CallbackInfo& info);
    shared_ptr<kuzu::main::QueryResult> queryResult;
};


#endif
