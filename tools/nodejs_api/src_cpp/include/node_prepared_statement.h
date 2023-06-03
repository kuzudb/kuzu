#pragma once

#include "binder/bound_statement_result.h"
#include "main/kuzu.h"
#include "node_connection.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/result/factorized_table.h"
#include <napi.h>

using namespace kuzu::main;

class NodePreparedStatement : public Napi::ObjectWrap<NodePreparedStatement> {
    friend class NodeConnection;
    friend class PreparedStatementInitAsyncWorker;

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodePreparedStatement(const Napi::CallbackInfo& info);
    ~NodePreparedStatement() = default;

private:
    Napi::Value InitAsync(const Napi::CallbackInfo& info);
    void InitCppPreparedStatement();
    Napi::Value IsSuccess(const Napi::CallbackInfo& info);
    Napi::Value GetErrorMessage(const Napi::CallbackInfo& info);


private:
    std::shared_ptr<PreparedStatement> preparedStatement;
    std::shared_ptr<Connection> connection;
    std::string queryString;
};

class PreparedStatementInitAsyncWorker : public Napi::AsyncWorker {
public:
    PreparedStatementInitAsyncWorker(
        Napi::Function& callback, NodePreparedStatement* nodePreparedStatement)
        : AsyncWorker(callback), nodePreparedStatement(nodePreparedStatement) {}

    ~PreparedStatementInitAsyncWorker() = default;

    inline void Execute() override {
        try {
            nodePreparedStatement->InitCppPreparedStatement();
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override { Callback().Call({Env().Null()}); }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodePreparedStatement* nodePreparedStatement;
};
