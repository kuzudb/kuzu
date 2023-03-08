#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeDatabase(const Napi::CallbackInfo& info);
    ~NodeDatabase();
    friend class NodeConnection;

private:
    void ResizeBufferManager(const Napi::CallbackInfo& info);
    static Napi::FunctionReference constructor;
    std::unique_ptr<kuzu::main::Database> database;
};
