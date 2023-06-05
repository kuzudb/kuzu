#pragma once

#include "binder/bound_statement_result.h"
#include "main/kuzu.h"
#include "node_util.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/result/factorized_table.h"
#include <napi.h>

using namespace kuzu::processor;
using namespace kuzu::main;

class NodeQueryResult : public Napi::ObjectWrap<NodeQueryResult> {
    friend class NodeQueryResultGetNextAsyncWorker;
    friend class NodeQueryResultGetColumnMetadataAsyncWorker;

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    NodeQueryResult(const Napi::CallbackInfo& info);
    void SetQueryResult(std::shared_ptr<QueryResult>& queryResult);
    ~NodeQueryResult() = default;

private:
    void ResetIterator(const Napi::CallbackInfo& info);
    Napi::Value HasNext(const Napi::CallbackInfo& info);
    Napi::Value GetNumTuples(const Napi::CallbackInfo& info);
    Napi::Value GetNextAsync(const Napi::CallbackInfo& info);
    Napi::Value GetColumnDataTypesAsync(const Napi::CallbackInfo& info);
    Napi::Value GetColumnNamesAsync(const Napi::CallbackInfo& info);

private:
    std::shared_ptr<QueryResult> queryResult;
};

enum GetColumnMetadataType { DATA_TYPE, NAME };

class NodeQueryResultGetColumnMetadataAsyncWorker : public Napi::AsyncWorker {
public:
    NodeQueryResultGetColumnMetadataAsyncWorker(
        Napi::Function& callback, NodeQueryResult* nodeQueryResult, GetColumnMetadataType type)
        : AsyncWorker(callback), nodeQueryResult(nodeQueryResult), type(type) {}

    ~NodeQueryResultGetColumnMetadataAsyncWorker() = default;

    inline void Execute() override {
        try {
            if (type == GetColumnMetadataType::DATA_TYPE) {
                auto columnDataTypes = nodeQueryResult->queryResult->getColumnDataTypes();
                result = std::vector<std::string>(columnDataTypes.size());
                for (auto i = 0u; i < columnDataTypes.size(); ++i) {
                    result[i] =
                        kuzu::common::LogicalTypeUtils::dataTypeToString(columnDataTypes[i]);
                }
            } else {
                result = nodeQueryResult->queryResult->getColumnNames();
            }
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override {
        auto env = Env();
        Napi::Array nodeResult = Napi::Array::New(env, result.size());
        for (auto i = 0u; i < result.size(); ++i) {
            nodeResult.Set(i, result[i]);
        }
        Callback().Call({env.Null(), nodeResult});
    }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeQueryResult* nodeQueryResult;
    GetColumnMetadataType type;
    std::vector<std::string> result;
};

class NodeQueryResultGetNextAsyncWorker : public Napi::AsyncWorker {
public:
    NodeQueryResultGetNextAsyncWorker(Napi::Function& callback, NodeQueryResult* nodeQueryResult)
        : AsyncWorker(callback), nodeQueryResult(nodeQueryResult) {}

    ~NodeQueryResultGetNextAsyncWorker() = default;

    inline void Execute() override {
        try {
            if (!nodeQueryResult->queryResult->hasNext()) {
                cppTuple.reset();
            }
            cppTuple = nodeQueryResult->queryResult->getNext();
        } catch (const std::exception& exc) { SetError(std::string(exc.what())); }
    }

    inline void OnOK() override {
        auto env = Env();
        if (cppTuple == nullptr) {
            Callback().Call({env.Null(), env.Undefined()});
            return;
        }
        Napi::Object nodeTuple = Napi::Object::New(env);
        try {
            auto columnNames = nodeQueryResult->queryResult->getColumnNames();
            for (auto i = 0; i < cppTuple->len(); ++i) {
                Napi::Value value = Util::ConvertToNapiObject(*cppTuple->getValue(i), env);
                nodeTuple.Set(columnNames[i], value);
            }
        } catch (const std::exception& exc) {
            auto napiError = Napi::Error::New(env, exc.what());
            Callback().Call({napiError.Value(), env.Undefined()});
        }
        Callback().Call({env.Null(), nodeTuple});
    }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeQueryResult* nodeQueryResult;
    std::shared_ptr<FlatTuple> cppTuple;
};
