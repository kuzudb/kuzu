#pragma once

#include "common/data_chunk/data_chunk.h"
#include "function.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

struct TableFuncSharedState {
    virtual ~TableFuncSharedState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TableFuncSharedState*, TARGET*>(this);
    }
};

struct TableFuncLocalState {
    virtual ~TableFuncLocalState() = default;
};

struct TableFuncInput {
    TableFuncBindData* bindData;
    TableFuncLocalState* localState;
    TableFuncSharedState* sharedState;

    TableFuncInput() = default;
    TableFuncInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
        TableFuncSharedState* sharedState)
        : bindData{bindData}, localState{localState}, sharedState{sharedState} {}
    DELETE_COPY_DEFAULT_MOVE(TableFuncInput);
};

// We are in the middle of merging different scan operators into table function. But they organize
// output vectors in different ways. E.g.
// - Call functions and scan file functions put all vectors into single data chunk
// - Factorized table scan instead
// We introduce this as a temporary solution to unify the interface. In the long term, we should aim
// to use ResultSet as TableFuncOutput.
struct TableFuncOutput {
    common::DataChunk dataChunk;
    std::vector<common::ValueVector*> vectors;

    TableFuncOutput() = default;
    DELETE_COPY_DEFAULT_MOVE(TableFuncOutput);
};

struct TableFunctionInitInput {
    TableFuncBindData* bindData;

    explicit TableFunctionInitInput(TableFuncBindData* bindData) : bindData{bindData} {}

    virtual ~TableFunctionInitInput() = default;
};

using table_func_bind_t = std::function<std::unique_ptr<TableFuncBindData>(main::ClientContext*,
    function::TableFuncBindInput*)>;
using table_func_t = std::function<common::offset_t(TableFuncInput&, TableFuncOutput&)>;
using table_func_init_shared_t =
    std::function<std::unique_ptr<TableFuncSharedState>(TableFunctionInitInput&)>;
using table_func_init_local_t = std::function<std::unique_ptr<TableFuncLocalState>(
    TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*)>;
using table_func_can_parallel_t = std::function<bool()>;
using table_func_progress_t = std::function<double(TableFuncSharedState* sharedState)>;

struct KUZU_API TableFunction : public Function {
    table_func_t tableFunc;
    table_func_bind_t bindFunc;
    table_func_init_shared_t initSharedStateFunc;
    table_func_init_local_t initLocalStateFunc;
    table_func_can_parallel_t canParallelFunc = [] { return true; };
    table_func_progress_t progressFunc = [](TableFuncSharedState* /*sharedState*/) { return 0.0; };

    TableFunction()
        : Function{}, tableFunc{nullptr}, bindFunc{nullptr}, initSharedStateFunc{nullptr},
          initLocalStateFunc{nullptr} {};
    TableFunction(std::string name, table_func_t tableFunc, table_func_bind_t bindFunc,
        table_func_init_shared_t initSharedFunc, table_func_init_local_t initLocalFunc,
        std::vector<common::LogicalTypeID> inputTypes)
        : Function{std::move(name), std::move(inputTypes)}, tableFunc{tableFunc},
          bindFunc{bindFunc}, initSharedStateFunc{initSharedFunc},
          initLocalStateFunc{initLocalFunc} {}
    TableFunction(std::string name, table_func_t tableFunc, table_func_bind_t bindFunc,
        table_func_init_shared_t initSharedFunc, table_func_init_local_t initLocalFunc,
        table_func_progress_t progressFunc, std::vector<common::LogicalTypeID> inputTypes)
        : Function{std::move(name), std::move(inputTypes)}, tableFunc{tableFunc},
          bindFunc{bindFunc}, initSharedStateFunc{initSharedFunc},
          initLocalStateFunc{initLocalFunc}, progressFunc{progressFunc} {}

    std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<TableFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu
