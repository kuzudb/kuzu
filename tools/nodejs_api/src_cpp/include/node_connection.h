#pragma once

#include <iostream>

#include "main/kuzu.h"
#include "node_database.h"
#include "node_prepared_statement.h"
#include "node_query_result.h"
#include <napi.h>

using namespace kuzu::main;
using namespace kuzu::common;

class NodeConnection : public Napi::ObjectWrap<NodeConnection> {
    friend class ConnectionInitAsyncWorker;
    friend class ConnectionTableMetadataAsyncWorker;
    friend class ConnectionExecuteAsyncWorker;
    friend class NodePreparedStatement;

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeConnection(const Napi::CallbackInfo& info);
    ~NodeConnection() = default;

private:
    Napi::Value InitAsync(const Napi::CallbackInfo& info);
    void InitCppConnection();
    void SetMaxNumThreadForExec(const Napi::CallbackInfo& info);
    Napi::Value GetNodeTableNamesAsync(const Napi::CallbackInfo& info);
    Napi::Value GetRelTableNamesAsync(const Napi::CallbackInfo& info);
    Napi::Value GetNodePropertyNamesAsync(const Napi::CallbackInfo& info);
    Napi::Value GetRelPropertyNamesAsync(const Napi::CallbackInfo& info);
    Napi::Value ExecuteAsync(const Napi::CallbackInfo& info);

private:
    std::shared_ptr<Database> database;
    std::shared_ptr<Connection> connection;
};

class ConnectionInitAsyncWorker : public Napi::AsyncWorker {
public:
    ConnectionInitAsyncWorker(Napi::Function& callback, NodeConnection* nodeConnection)
        : Napi::AsyncWorker(callback), nodeConnection(nodeConnection) {}

    ~ConnectionInitAsyncWorker() = default;

    inline void Execute() override {
        try {
            nodeConnection->InitCppConnection();
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override { Callback().Call({Env().Null()}); }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeConnection* nodeConnection;
};

enum GetTableMetadataType {
    NODE_TABLE_NAME,
    REL_TABLE_NAME,
    NODE_PROPERTY_NAME,
    REL_PROPERTY_NAME
};

class ConnectionTableMetadataAsyncWorker : public Napi::AsyncWorker {
public:
    ConnectionTableMetadataAsyncWorker(Napi::Function& callback, NodeConnection* nodeConnection,
        std::string tableName, GetTableMetadataType getTableMetadataType)
        : AsyncWorker(callback), nodeConnection(nodeConnection), tableName(tableName),
          getTableMetadataType(getTableMetadataType) {}

    ~ConnectionTableMetadataAsyncWorker() = default;

    inline void Execute() override {
        try {
            switch (getTableMetadataType) {
            case NODE_TABLE_NAME:
                result = nodeConnection->connection->getNodeTableNames();
                break;
            case REL_TABLE_NAME:
                result = nodeConnection->connection->getRelTableNames();
                break;
            case NODE_PROPERTY_NAME:
                result = nodeConnection->connection->getNodePropertyNames(tableName);
                break;
            case REL_PROPERTY_NAME:
                result = nodeConnection->connection->getRelPropertyNames(tableName);
                break;
            }
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override {
        Callback().Call({Env().Null(), Napi::String::New(Env(), result)});
    }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeConnection* nodeConnection;
    std::string tableName;
    std::string result;
    GetTableMetadataType getTableMetadataType;
};

class ConnectionExecuteAsyncWorker : public Napi::AsyncWorker {
public:
    ConnectionExecuteAsyncWorker(Napi::Function& callback, std::shared_ptr<Connection>& connection,
        std::shared_ptr<PreparedStatement> preparedStatement, NodeQueryResult* nodeQueryResult,
        std::unordered_map<std::string, std::shared_ptr<Value>>& params)
        : Napi::AsyncWorker(callback), preparedStatement(preparedStatement),
          nodeQueryResult(nodeQueryResult), connection(connection), params(params) {}
    ~ConnectionExecuteAsyncWorker() = default;

    inline void Execute() override {
        try {
            std::shared_ptr<QueryResult> result =
                std::move(connection->executeWithParams(preparedStatement.get(), params));
            nodeQueryResult->SetQueryResult(result);
            if (!result->isSuccess()) {
                SetError(result->getErrorMessage());
                return;
            }
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override { Callback().Call({Env().Null()}); }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    std::shared_ptr<Connection> connection;
    std::shared_ptr<PreparedStatement> preparedStatement;
    NodeQueryResult* nodeQueryResult;
    std::unordered_map<std::string, std::shared_ptr<Value>> params;
};
