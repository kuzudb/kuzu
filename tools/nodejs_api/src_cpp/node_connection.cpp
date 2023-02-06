#include "node_connection.h"
#include "node_database.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeConnection::constructor;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

  Napi::Function t = DefineClass(env, "NodeConnection", {
      InstanceMethod("execute", &NodeConnection::Execute),
      InstanceMethod("setMaxNumThreadForExec", &NodeConnection::SetMaxNumThreadForExec),
      InstanceMethod("getNodePropertyNames", &NodeConnection::GetNodePropertyNames),
  });

    constructor = Napi::Persistent(t);
    constructor.SuppressDestruct();

    exports.Set("NodeConnection", t);
    return exports;
}


NodeConnection::NodeConnection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeConnection>(info)  {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length()!=2 || !info[0].IsObject() || !info[1].IsNumber()) {
      Napi::TypeError::New(env, "Need a valid database class passed in").ThrowAsJavaScriptException();
      return;
  }
  NodeDatabase * nodeDatabase = NodeDatabase::Unwrap(info[0].As<Napi::Object>());
  uint64_t numThreads = info[1].As<Napi::Number>().DoubleValue();

  try {
      this->connection = make_unique<kuzu::main::Connection>(nodeDatabase->database.get());
      if (numThreads > 0) {
          this->connection->setMaxNumThreadForExec(numThreads);
      }
  }
  catch(const std::exception &exc){
      Napi::TypeError::New(env, "Unsuccessful Connection Initialization: " + std::string(exc.what())).ThrowAsJavaScriptException();
  }
}

NodeConnection::~NodeConnection() {}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  // add parsing for queries TODO: make this smart? like check whether a user created before matching???
  std::string query = "";
  if (info.Length()>0) {
    query = info[0].ToString();
    if (!query.starts_with("MATCH") && !query.starts_with("create") && !query.starts_with("COPY")) {
        Napi::TypeError::New(env, "Wrong arguments").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

  std::unique_ptr<kuzu::main::QueryResult> queryResult = this->connection->query(query);
  if (!queryResult->isSuccess()) {
      Napi::TypeError::New(env, "Unsuccessful execute: " + queryResult->getErrorMessage()).ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  Napi::Object output = Napi::Object::New(env);
  int i = 0;
  while (queryResult->hasNext()) {
      auto row = queryResult->getNext();
      Napi::Array obj = Napi::Array::New(env, row->len());
      for (int j=0; j < row->len(); j++) {
          obj.Set(j, row->getValue(j)->toString());
      }
      output.Set(uint32_t(i), obj);
      ++i;
  }
  return output;
}

void NodeConnection::SetMaxNumThreadForExec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length()!=1 || !info[0].IsNumber()) {
      Napi::TypeError::New(env, "Need Integer Number of Threads as an argument").ThrowAsJavaScriptException();
      return;
  }
  uint64_t numThreads = info[0].ToNumber().DoubleValue();
  try {
      this->connection->setMaxNumThreadForExec(numThreads);
  }
  catch(const std::exception &exc) {
      Napi::TypeError::New(env, "Unsuccessful setMaxNumThreadForExec: " + std::string(exc.what())).ThrowAsJavaScriptException();
  }
  return;
}

Napi::Value NodeConnection::GetNodePropertyNames(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length()!=1 || !info[0].IsString()) {
      Napi::TypeError::New(env, "Need Table Name as an argument").ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  std::string tableName = info[0].ToString();
  std::string propertyNames;
  try {
      propertyNames = this->connection->getNodePropertyNames(tableName);
  }
  catch(const std::exception &exc) {
      Napi::TypeError::New(env, "Unsuccessful getNodePropertyNames: " + std::string(exc.what())).ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  return Napi::String::New(env, propertyNames);;
}
