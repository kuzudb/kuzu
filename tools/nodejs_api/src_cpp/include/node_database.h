#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

using namespace std;

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeDatabase(const Napi::CallbackInfo& info);
    ~NodeDatabase() = default;
    friend class NodeConnection;

private:
    shared_ptr<kuzu::main::Database> database;
};
