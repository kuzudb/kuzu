#include "include/node_database.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase", {
          InstanceMethod("resizeBufferManager", &NodeDatabase::ResizeBufferManager),
      });

    exports.Set("NodeDatabase", t);
    return exports;
}


NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeDatabase>(info)  {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    std::string databasePath = info[0].ToString();
    std::int64_t bufferPoolSize = info[1].As<Napi::Number>().DoubleValue();

    auto systemConfig = kuzu::main::SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.defaultPageBufferPoolSize =
            bufferPoolSize * kuzu::common::StorageConstants::DEFAULT_PAGES_BUFFER_RATIO;
        systemConfig.largePageBufferPoolSize =
            bufferPoolSize * kuzu::common::StorageConstants::LARGE_PAGES_BUFFER_RATIO;
    }

    try {
        this->database = make_unique<kuzu::main::Database>(databasePath, systemConfig);
    } catch(const std::exception &exc) {
        Napi::Error::New(env, "Unsuccessful Database Initialization: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

void NodeDatabase::ResizeBufferManager(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    std::int64_t bufferSize = info[0].As<Napi::Number>().DoubleValue();

    try {
        this->database->resizeBufferManager(bufferSize);
    } catch(const std::exception &exc) {
        Napi::Error::New(env, "Unsuccessful resizeBufferManager: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return;
}
