#include "include/node_database.h"


Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase",
        {
            InstanceMethod("initAsync", &NodeDatabase::InitAsync),
            InstanceMethod("setLoggingLevel", &NodeDatabase::setLoggingLevel),
        });

    exports.Set("NodeDatabase", t);
    return exports;
}

NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeDatabase>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    databasePath = info[0].ToString();
    bufferPoolSize = info[1].As<Napi::Number>().Int64Value();
}

Napi::Value NodeDatabase::InitAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto callback = info[0].As<Napi::Function>();

    auto* asyncWorker = new DatabaseInitAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

void NodeDatabase::InitCppDatabase() {
    if (bufferPoolSize) {
        auto systemConfig = SystemConfig();
        systemConfig.bufferPoolSize = bufferPoolSize;
        this->database = make_shared<Database>(databasePath, systemConfig);
    } else {
        this->database = make_shared<Database>(databasePath);
    }
}

void NodeDatabase::setLoggingLevel(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto loggingLevel = info[0].As<Napi::String>().Utf8Value();
    database->setLoggingLevel(std::move(loggingLevel));
}
