#include "include/node_query_result.h"

#include <thread>

#include "include/node_util.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult",
        {InstanceMethod("resetIterator", &NodeQueryResult::ResetIterator),
            InstanceMethod("hasNext", &NodeQueryResult::HasNext),
            InstanceMethod("hasNextQueryResult", &NodeQueryResult::HasNextQueryResult),
            InstanceMethod("getNextQueryResultAsync", &NodeQueryResult::GetNextQueryResultAsync),
            InstanceMethod("getNextQueryResultSync", &NodeQueryResult::GetNextQueryResultSync),
            InstanceMethod("getNumTuples", &NodeQueryResult::GetNumTuples),
            InstanceMethod("getNextSync", &NodeQueryResult::GetNextSync),
            InstanceMethod("getNextAsync", &NodeQueryResult::GetNextAsync),
            InstanceMethod("getColumnDataTypesAsync", &NodeQueryResult::GetColumnDataTypesAsync),
            InstanceMethod("getColumnDataTypesSync", &NodeQueryResult::GetColumnDataTypesSync),
            InstanceMethod("getColumnNamesAsync", &NodeQueryResult::GetColumnNamesAsync),
            InstanceMethod("getColumnNamesSync", &NodeQueryResult::GetColumnNamesSync),
            InstanceMethod("close", &NodeQueryResult::Close)});

    exports.Set("NodeQueryResult", t);
    return exports;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeQueryResult>(info) {}

NodeQueryResult::~NodeQueryResult() {
    this->Close();
}

void NodeQueryResult::SetQueryResult(QueryResult* queryResult, bool isOwned) {
    this->queryResult = queryResult;
    this->isOwned = isOwned;
}

void NodeQueryResult::ResetIterator(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        this->queryResult->resetIterator();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

Napi::Value NodeQueryResult::HasNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Boolean::New(env, this->queryResult->hasNext());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::HasNextQueryResult(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Boolean::New(env, this->queryResult->hasNextQueryResult());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextQueryResultAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto newQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[0].As<Napi::Object>());
    auto callback = info[1].As<Napi::Function>();
    auto* asyncWorker =
        new NodeQueryResultGetNextQueryResultAsyncWorker(callback, this, newQueryResult);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextQueryResultSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        auto nextResult = this->queryResult->getNextQueryResult();
        if (!nextResult->isSuccess()) {
            Napi::Error::New(env, nextResult->getErrorMessage()).ThrowAsJavaScriptException();
        }
        auto nodeQueryResult =
            Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[0].As<Napi::Object>());
        nodeQueryResult->SetQueryResult(nextResult, false);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNumTuples(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Number::New(env, this->queryResult->getNumTuples());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetNextAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        if (!this->queryResult->hasNext()) {
            return env.Null();
        }
        auto cppTuple = this->queryResult->getNext();
        Napi::Object nodeTuple = Napi::Object::New(env);
        PopulateColumnNames();
        for (auto i = 0u; i < cppTuple->len(); ++i) {
            Napi::Value value = Util::ConvertToNapiObject(*cppTuple->getValue(i), env);
            nodeTuple.Set(columnNames->at(i), value);
        }
        return nodeTuple;
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeQueryResult::GetColumnDataTypesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(callback, this,
        GetColumnMetadataType::DATA_TYPE);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetColumnDataTypesSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        auto columnDataTypes = this->queryResult->getColumnDataTypes();
        Napi::Array nodeColumnDataTypes = Napi::Array::New(env, columnDataTypes.size());
        for (auto i = 0u; i < columnDataTypes.size(); ++i) {
            nodeColumnDataTypes.Set(i, Napi::String::New(env, columnDataTypes[i].toString()));
        }
        return nodeColumnDataTypes;
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeQueryResult::GetColumnNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(callback, this,
        GetColumnMetadataType::NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetColumnNamesSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    PopulateColumnNames();
    try {
        Napi::Array nodeColumnNames = Napi::Array::New(env, columnNames->size());
        for (auto i = 0u; i < columnNames->size(); ++i) {
            nodeColumnNames.Set(i, Napi::String::New(env, columnNames->at(i)));
        }
        return nodeColumnNames;
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

void NodeQueryResult::PopulateColumnNames() {
    if (this->columnNames != nullptr) {
        return;
    }
    this->columnNames =
        std::make_unique<std::vector<std::string>>(this->queryResult->getColumnNames());
}

void NodeQueryResult::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    this->Close();
}

void NodeQueryResult::Close() {
    if (this->isOwned) {
        delete this->queryResult;
        this->queryResult = nullptr;
    }
}
