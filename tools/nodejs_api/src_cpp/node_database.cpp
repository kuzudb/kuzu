#include "include/node_database.h"

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase",
        {
            InstanceMethod("initAsync", &NodeDatabase::InitAsync),
            InstanceMethod("setLoggingLevel", &NodeDatabase::setLoggingLevel),
            StaticMethod("getVersion", &NodeDatabase::GetVersion),
            StaticMethod("getStorageVersion", &NodeDatabase::GetStorageVersion),
        });

    exports.Set("NodeDatabase", t);
    return exports;
}

NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeDatabase>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    databasePath = info[0].ToString();
    bufferPoolSize = info[1].As<Napi::Number>().Int64Value();
    enableCompression = info[2].As<Napi::Boolean>().Value();
    readOnly = info[3].As<Napi::Boolean>().Value();
    maxDBSize = info[4].As<Napi::Number>().Int64Value();
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
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.bufferPoolSize = bufferPoolSize;
    }
    if (!enableCompression) {
        systemConfig.enableCompression = enableCompression;
    }
    systemConfig.readOnly = readOnly;
    if (maxDBSize > 0) {
        systemConfig.maxDBSize = maxDBSize;
    }
    this->database = std::make_shared<Database>(databasePath, systemConfig);
}

void NodeDatabase::setLoggingLevel(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto loggingLevel = info[0].As<Napi::String>().Utf8Value();
    database->setLoggingLevel(std::move(loggingLevel));
}

Napi::Value NodeDatabase::GetVersion(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::String::New(env, Version::getVersion());
}

Napi::Value NodeDatabase::GetStorageVersion(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::Number::New(env, Version::getStorageVersion());
}
