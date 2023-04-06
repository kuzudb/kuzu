#include <napi.h>
#include <iostream>
#include "main/kuzu.h"

using namespace std;

class NodeConnection : public Napi::ObjectWrap<NodeConnection> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  NodeConnection(const Napi::CallbackInfo& info);
  ~NodeConnection() = default;

 private:
  Napi::Value GetConnection(const Napi::CallbackInfo& info);
  Napi::Value TransferConnection(const Napi::CallbackInfo& info);
  Napi::Value Execute(const Napi::CallbackInfo& info);
  void SetMaxNumThreadForExec(const Napi::CallbackInfo& info);
  Napi::Value GetNodePropertyNames(const Napi::CallbackInfo& info);
  shared_ptr<kuzu::main::Database> database;
  shared_ptr<kuzu::main::Connection> connection;
  uint64_t numThreads = 0;

  struct ThreadSafeConnectionContext {
      ThreadSafeConnectionContext(Napi::Env env, shared_ptr<kuzu::main::Connection> & connection,
          shared_ptr<kuzu::main::Database> & database, uint64_t numThreads) :
            deferred(Napi::Promise::Deferred::New(env)),
            connection(connection),
            database(database),
            numThreads(numThreads){};

      // Native Promise returned to JavaScript
      Napi::Promise::Deferred deferred;

      uint64_t numThreads = 0;

      bool passed = true;

      shared_ptr<kuzu::main::Connection> connection;
      kuzu::main::Connection * connection2;
      shared_ptr<kuzu::main::Database> database;

      // Native thread
      std::thread nativeThread;

      Napi::ThreadSafeFunction tsfn;
  };
  ThreadSafeConnectionContext * context;
  static void threadEntry(ThreadSafeConnectionContext * context);
  static void FinalizerCallback(Napi::Env env, void* finalizeData, ThreadSafeConnectionContext* context);
};
