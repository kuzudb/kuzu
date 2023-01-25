#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class ClassExample : public Napi::ObjectWrap<ClassExample> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  ClassExample(const Napi::CallbackInfo& info);

 private:
  static Napi::FunctionReference constructor;
  Napi::Value Temp(const Napi::CallbackInfo& info);
  kuzu::main::Database *database_;
  kuzu::main::Connection *connection_;
};
