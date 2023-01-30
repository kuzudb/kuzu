#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class NodeConnection : public Napi::ObjectWrap<NodeConnection> {
<<<<<<< HEAD
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeConnection(const Napi::CallbackInfo& info);
    ~NodeConnection();
private:
    static Napi::FunctionReference constructor;
    Napi::Value Execute(const Napi::CallbackInfo& info);
    kuzu::main::Connection *connection_;
=======
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  NodeConnection(const Napi::CallbackInfo& info);
  ~NodeConnection();
 private:
  static Napi::FunctionReference constructor;
  Napi::Value Execute(const Napi::CallbackInfo& info);
  kuzu::main::Connection *connection_;
>>>>>>> added Database Class, all its methods, database java class, and added it to connection
};
