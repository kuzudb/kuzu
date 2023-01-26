#include "node_connection.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeConnection::constructor;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
  Napi::HandleScope scope(env);

  Napi::Function t = DefineClass(env, "NodeConnection", {
      InstanceMethod("execute", &NodeConnection::Execute),
  });

  constructor = Napi::Persistent(t);
  constructor.SuppressDestruct();

  exports.Set("NodeConnection", t);
  return exports;
}


NodeConnection::NodeConnection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeConnection>(info)  {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length()!=1) {
      Napi::TypeError::New(env, "Need database config string (of length 1)").ThrowAsJavaScriptException();
  }
  if (!info[0].IsString()) {
      Napi::TypeError::New(env, "Database config parameter must be a string").ThrowAsJavaScriptException();
  }

  std::string databaseConfigString = info[0].ToString().Utf8Value().c_str();
  std::cout << databaseConfigString << std::endl;
  DatabaseConfig databaseConfig(databaseConfigString);
  SystemConfig systemConfig(1ull << 30 /* set buffer manager size to 2GB */);
  Database * database = new Database(databaseConfig, systemConfig);

  this->database_ = database;

  auto connection = new Connection(this->database_);
  this->connection_ = connection;
}

NodeConnection::~NodeConnection() {
  delete this->database_;
  delete this->connection_;
}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  // add parsing for queries TODO: make this smart? like check whether a user created before matching???
  std::string query = "";
  if (info.Length()>0) {
    query = info[0].ToString().Utf8Value().c_str();
    if (!query.starts_with("MATCH") && !query.starts_with("create") && !query.starts_with("COPY")) {
        Napi::TypeError::New(env, "Wrong arguments").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }
  }

  std::cout << "Arg: " << query << std::endl;

  auto result = this->connection_->query(query);

  Napi::Object output = Napi::Object::New(env);
  if (query.starts_with("MATCH")) { // TODO: make this check more robust go through all return types for queries
    auto i = 0;
    while (result->hasNext()) {
        auto row = result->getNext();
        Napi::Object obj = Napi::Object::New(env);
        std::string fName = row->getResultValue(0)->getValue<std::string>();
        obj.Set("fName", Napi::String::New(env, fName));
        obj.Set("gender", Napi::Number::New(env, row->getResultValue(1)->getValue<int64_t>()));
        obj.Set("eyeSight", Napi::Number::New(env, row->getResultValue(2)->getValue<double>()));
        obj.Set("isStudent", row->getResultValue(3)->getValue<bool>());
        output.Set(uint32_t(i), obj);
        ++i;
    }
  }
  return output;
}