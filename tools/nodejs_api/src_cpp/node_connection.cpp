#include "node_connection.h"

#include "execute_async_worker.h"
#include "main/kuzu.h"
#include "node_database.h"
#include "node_query_result.h"

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
  NodeDatabase * nodeDatabase = Napi::ObjectWrap<NodeDatabase>::Unwrap(info[0].As<Napi::Object>());
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

  if (info.Length()==2 && (!info[0].IsString() || !info[1].IsFunction())) {
      Napi::TypeError::New(env, "Execute needs query parameter & callback").ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  std::string query = info[0].ToString();
  Function callback = info[1].As<Function>();
  ExecuteAsyncWorker * asyncWorker = new ExecuteAsyncWorker(callback, connection, query);
  asyncWorker->Queue();
  return info.Env().Undefined();
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
