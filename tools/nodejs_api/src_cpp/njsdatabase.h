#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class NjsDatabase : public Napi::ObjectWrap<NjsDatabase> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  NjsDatabase(const Napi::CallbackInfo& info);
  ~NjsDatabase();
 private:
  static Napi::FunctionReference constructor;
  Napi::Value Temp(const Napi::CallbackInfo& info);
  kuzu::main::Database *database_;
  kuzu::main::Connection *connection_;
};
