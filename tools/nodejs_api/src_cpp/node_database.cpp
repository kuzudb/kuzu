#include "include/node_database.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase", {
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
        systemConfig.bufferPoolSize = bufferPoolSize;
    }

    try {
        this->database = make_shared<kuzu::main::Database>(databasePath, systemConfig);
    } catch(const std::exception &exc) {
        Napi::Error::New(env, "Unsuccessful Database Initialization: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
}
