#pragma once

#include "common/data_chunk/data_chunk.h"
#include "function.h"

namespace kuzu {
namespace catalog {
class Catalog;
} // namespace catalog
namespace common {
class ValueVector;
}
namespace main {
class ClientContext;
}
namespace storage {
class StorageManager;
}

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

struct TableFuncSharedState {
    virtual ~TableFuncSharedState() = default;
};

struct TableFuncLocalState {
    virtual ~TableFuncLocalState() = default;
};

struct TableFunctionInput {
    TableFuncBindData* bindData;
    TableFuncLocalState* localState;
    TableFuncSharedState* sharedState;

    TableFunctionInput() = default;
    TableFunctionInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
        TableFuncSharedState* sharedState)
        : bindData{bindData}, localState{localState}, sharedState{sharedState} {}
};

struct TableFunctionInitInput {
    TableFuncBindData* bindData;

    explicit TableFunctionInitInput(TableFuncBindData* bindData) : bindData{bindData} {}

    virtual ~TableFunctionInitInput() = default;
};

typedef std::unique_ptr<TableFuncBindData> (*table_func_bind_t)(main::ClientContext* /*context*/,
    TableFuncBindInput* /*input*/, catalog::Catalog* /*catalog*/,
    storage::StorageManager* /*storageManager*/);
typedef void (*table_func_t)(TableFunctionInput& data, common::DataChunk& output);
typedef std::unique_ptr<TableFuncSharedState> (*table_func_init_shared_t)(
    TableFunctionInitInput& input);
typedef std::unique_ptr<TableFuncLocalState> (*table_func_init_local_t)(
    TableFunctionInitInput& input, TableFuncSharedState* state, storage::MemoryManager* mm);
typedef bool (*table_func_can_parallel_t)();

struct TableFunction : public Function {
    table_func_t tableFunc;
    table_func_bind_t bindFunc;
    table_func_init_shared_t initSharedStateFunc;
    table_func_init_local_t initLocalStateFunc;
    table_func_can_parallel_t canParallelFunc = [] { return true; };

    TableFunction(std::string name, table_func_t tableFunc, table_func_bind_t bindFunc,
        table_func_init_shared_t initSharedFunc, table_func_init_local_t initLocalFunc,
        std::vector<common::LogicalTypeID> inputTypes)
        : Function{FunctionType::TABLE, std::move(name), std::move(inputTypes)},
          tableFunc{tableFunc}, bindFunc{bindFunc}, initSharedStateFunc{initSharedFunc},
          initLocalStateFunc{initLocalFunc} {}

    inline std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<TableFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu
