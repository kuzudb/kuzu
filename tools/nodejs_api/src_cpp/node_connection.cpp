#include "node_connection.h"
#include "node_database.h"

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

  if (info.Length()!=1 || !info[0].IsObject()) {
      Napi::TypeError::New(env, "Need database class passed in").ThrowAsJavaScriptException();
      return;
  }
  NodeDatabase * nodeDatabase = NodeDatabase::Unwrap(info[0].As<Napi::Object>());

  try {
      auto connection = new Connection(nodeDatabase->database_);
      this->connection_ = connection;
  }
  catch(const std::exception &exc){
      Napi::TypeError::New(env, exc.what()).ThrowAsJavaScriptException();
  }
}

NodeConnection::~NodeConnection() {
  delete this->connection_;
}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length()!=1 || !info[0].IsString()) {
      Napi::TypeError::New(env, "Execute needs query parameter").ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  std::string query = info[0].ToString();

  std::unique_ptr<kuzu::main::QueryResult> queryResult = this->connection_->query(query);
  if (!queryResult->isSuccess()) {
      Napi::TypeError::New(env, queryResult->getErrorMessage()).ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  Napi::Object output = Napi::Object::New(env);
  if (query.starts_with("MATCH")) { // TODO: make this check more robust go through all return types for queries
    auto i = 0;
    while (queryResult->hasNext()) {
        auto row = queryResult->getNext();
        Napi::Object obj = Napi::Object::New(env);
        std::string fName = row->getValue(0)->toString();
        obj.Set("fName", Napi::String::New(env, fName));
        obj.Set("gender", Napi::Number::New(env, std::stoull(row->getValue(1)->toString())));
        obj.Set("eyeSight", Napi::Number::New(env, std::stod(row->getValue(2)->toString())));
        obj.Set("isStudent", row->getValue(3)->toString());
        output.Set(uint32_t(i), obj);
        ++i;
    }
  }
  return output;
}