#include "node_database.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeDatabase::constructor;

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase", {
          InstanceMethod("resizeBufferManager", &NodeDatabase::ResizeBufferManager),
      });

    constructor = Napi::Persistent(t);
    constructor.SuppressDestruct();

    exports.Set("NodeDatabase", t);
    return exports;
}


NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeDatabase>(info)  {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=2) {
        Napi::TypeError::New(env, "Need database config string and buffer manager size").ThrowAsJavaScriptException();
    }
    if (!info[0].IsString()) {
        Napi::TypeError::New(env, "Database config parameter must be a string").ThrowAsJavaScriptException();
    }
    if (!info[1].IsNumber()) {
        Napi::TypeError::New(env, "Database buffer manager size must be an int_64").ThrowAsJavaScriptException();
    }

    std::string databaseConfigString = info[0].ToString().Utf8Value().c_str();
    std::cout << "Database config is " << databaseConfigString << std::endl;
    DatabaseConfig databaseConfig(databaseConfigString);

    std::int64_t bufferSize = info[1].As<Napi::Number>().DoubleValue();
    std::cout  << "Database buffer size is " << bufferSize << std::endl;
    SystemConfig systemConfig(bufferSize);

    Database * database = new Database(databaseConfig, systemConfig);

    this->database_ = database;
}

NodeDatabase::~NodeDatabase() {
    delete this->database_;
}

void NodeDatabase::ResizeBufferManager(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    // add parsing for queries TODO: make this smart?
    std::int64_t bufferSize = 0;
    if (info.Length()!=1 || !info[0].IsNumber()) {
        Napi::TypeError::New(env, "Database buffer manager size must be an int_64").ThrowAsJavaScriptException();
    }
    bufferSize = info[0].As<Napi::Number>().DoubleValue();

    this->database_->resizeBufferManager(bufferSize);

    return;
}