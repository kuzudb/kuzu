#include "include/node_connection.h"

#include "include/execute_async_worker.h"
#include "include/node_database.h"
#include "include/node_query_result.h"
#include "main/kuzu.h"
#include "include/util.h"

using namespace kuzu::main;

Napi::FunctionReference NodeConnection::constructor;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
  Napi::HandleScope scope(env);

  Napi::Function t = DefineClass(env, "NodeConnection", {
      InstanceMethod("execute", &NodeConnection::Execute),
      InstanceMethod("setMaxNumThreadForExec", &NodeConnection::SetMaxNumThreadForExec),
      InstanceMethod("getNodePropertyNames", &NodeConnection::GetNodePropertyNames),
  });

  exports.Set("NodeConnection", t);
  return exports;
}


NodeConnection::NodeConnection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeConnection>(info)  {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  NodeDatabase * nodeDatabase = Napi::ObjectWrap<NodeDatabase>::Unwrap(info[0].As<Napi::Object>());
  uint64_t numThreads = info[1].As<Napi::Number>().DoubleValue();

  try {
      this->connection = make_unique<kuzu::main::Connection>(nodeDatabase->database.get());
      if (numThreads > 0) {
          this->connection->setMaxNumThreadForExec(numThreads);
      }
  } catch(const std::exception &exc){
      Napi::TypeError::New(env, "Unsuccessful Connection Initialization: " + std::string(exc.what())).ThrowAsJavaScriptException();
  }
}

NodeConnection::~NodeConnection() {}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  std::string query = info[0].ToString();
  Napi::Function callback = info[1].As<Napi::Function>();
  NodeQueryResult * nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[2].As<Napi::Object>());
  auto params = Util::transformParameters(info[3].As<Napi::Array>());
  try {
      ExecuteAsyncWorker* asyncWorker = new ExecuteAsyncWorker(callback, connection, query, nodeQueryResult, params);
      asyncWorker->Queue();
  } catch(const std::exception &exc) {
      Napi::TypeError::New(env, "Unsuccessful execute: " + std::string(exc.what())).ThrowAsJavaScriptException();
  }
  return info.Env().Undefined();
}

void NodeConnection::SetMaxNumThreadForExec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  uint64_t numThreads = info[0].ToNumber().DoubleValue();
  try {
      this->connection->setMaxNumThreadForExec(numThreads);
  } catch(const std::exception &exc) {
      Napi::TypeError::New(env, "Unsuccessful setMaxNumThreadForExec: " + std::string(exc.what())).ThrowAsJavaScriptException();
  }
  return;
}

Napi::Value NodeConnection::GetNodePropertyNames(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  std::string tableName = info[0].ToString();
  std::string propertyNames;
  try {
      propertyNames = this->connection->getNodePropertyNames(tableName);
  } catch(const std::exception &exc) {
      Napi::TypeError::New(env, "Unsuccessful getNodePropertyNames: " + std::string(exc.what())).ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  return Napi::String::New(env, propertyNames);;
}
