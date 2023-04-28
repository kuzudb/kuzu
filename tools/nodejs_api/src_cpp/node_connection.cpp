#include "include/node_connection.h"

#include "include/execute_async_worker.h"
#include "include/node_database.h"
#include "include/node_query_result.h"
#include "main/kuzu.h"
#include "include/util.h"

using namespace kuzu::main;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
  Napi::HandleScope scope(env);

  Napi::Function t = DefineClass(env, "NodeConnection", {
      InstanceMethod("getConnection", &NodeConnection::GetConnection),
      InstanceMethod("transferConnection", &NodeConnection::TransferConnection),
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
  database = nodeDatabase->database;
  numThreads = info[1].As<Napi::Number>().DoubleValue();
}

void NodeConnection::threadEntry(ThreadSafeConnectionContext * context) {
  try {
      context->connection = make_shared<kuzu::main::Connection>(context->database.get());
      if (context->numThreads > 0) {
          context->connection->setMaxNumThreadForExec(context->numThreads);
      }
  } catch(const std::exception &exc){
      context->passed = false;
  }

  context->tsfn.Release();
}

void NodeConnection::FinalizerCallback(Napi::Env env, void* finalizeData, ThreadSafeConnectionContext * context) {
  context->nativeThread.join();

  context->deferred.Resolve(Napi::Boolean::New(env, context->passed));
}

Napi::Value Fn(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  return Napi::String::New(env, "Hello World");
}


Napi::Value NodeConnection::GetConnection(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  context = new ThreadSafeConnectionContext(env, connection, database, numThreads);

  context->tsfn = Napi::ThreadSafeFunction::New(
      env,                           // Environment
      Napi::Function::New<Fn>(env),  // JS function from caller
      "ThreadSafeConnectionContext", // Resource name
      0,                             // Max queue size (0 = unlimited).
      1,                             // Initial thread count
      context,                       // Context,
      FinalizerCallback,             // Finalizer
      (void*)nullptr                 // Finalizer data
  );
  context->nativeThread = std::thread(threadEntry, context);

  return context->deferred.Promise();
}

Napi::Value NodeConnection::TransferConnection(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  connection = std::move(context->connection);
  delete context;
  return info.Env().Undefined();
}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  std::string query = info[0].ToString();
  Napi::Function callback = info[1].As<Napi::Function>();
  NodeQueryResult * nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[2].As<Napi::Object>());
  try {
      auto params = Util::transformParameters(info[3].As<Napi::Array>());
      ExecuteAsyncWorker* asyncWorker = new ExecuteAsyncWorker(callback, connection, query, nodeQueryResult, params);
      asyncWorker->Queue();
  } catch(const std::exception &exc) {
      Napi::Error::New(env, "Unsuccessful execute: " + std::string(exc.what())).ThrowAsJavaScriptException();
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
      Napi::Error::New(env, "Unsuccessful setMaxNumThreadForExec: " + std::string(exc.what())).ThrowAsJavaScriptException();
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
      Napi::Error::New(env, "Unsuccessful getNodePropertyNames: " + std::string(exc.what())).ThrowAsJavaScriptException();
      return Napi::Object::New(env);
  }
  return Napi::String::New(env, propertyNames);;
}
