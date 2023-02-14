#ifndef KUZU_NODE_QUERY_RESULT_H
#define KUZU_NODE_QUERY_RESULT_H

#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class NodeQueryResult: public Napi::ObjectWrap<NodeQueryResult> {
 public:
  static Napi::Object Wrap(Napi::Env env, unique_ptr<kuzu::main::QueryResult> & queryResult);
  NodeQueryResult(const Napi::CallbackInfo& info);
  ~NodeQueryResult();

 private:
    static Napi::FunctionReference constructor;
    Napi::Value HasNext(const Napi::CallbackInfo& info);
    Napi::Value GetNext(const Napi::CallbackInfo& info);
    void Close(const Napi::CallbackInfo& info);
    unique_ptr<kuzu::main::QueryResult> queryResult;
};


#endif
