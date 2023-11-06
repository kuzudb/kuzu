#pragma once

#include <memory>

#include "main/kuzu.h"
#include <napi.h>

using namespace kuzu::main;

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
    friend class NodeConnection;
    friend class DatabaseInitAsyncWorker;

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeDatabase(const Napi::CallbackInfo& info);
    ~NodeDatabase() = default;

private:
    Napi::Value InitAsync(const Napi::CallbackInfo& info);
    void InitCppDatabase();
    void setLoggingLevel(const Napi::CallbackInfo& info);

private:
    std::string databasePath;
    size_t bufferPoolSize;
    bool enableCompression;
    kuzu::common::AccessMode accessMode;
    std::shared_ptr<Database> database;
};

class DatabaseInitAsyncWorker : public Napi::AsyncWorker {
public:
    DatabaseInitAsyncWorker(Napi::Function& callback, NodeDatabase* nodeDatabase)
        : AsyncWorker(callback), nodeDatabase(nodeDatabase) {}

    ~DatabaseInitAsyncWorker() = default;

    inline void Execute() override {
        try {
            nodeDatabase->InitCppDatabase();

        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override { Callback().Call({Env().Null()}); }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeDatabase* nodeDatabase;
};
