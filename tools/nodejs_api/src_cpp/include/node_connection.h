#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class NodeConnection : public Napi::ObjectWrap<NodeConnection> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  NodeConnection(const Napi::CallbackInfo& info);
  ~NodeConnection();

 private:
  static Napi::FunctionReference constructor;
  Napi::Value Execute(const Napi::CallbackInfo& info);
  void SetMaxNumThreadForExec(const Napi::CallbackInfo& info);
  Napi::Value GetNodePropertyNames(const Napi::CallbackInfo& info);
  shared_ptr<kuzu::main::Connection> connection;
};
