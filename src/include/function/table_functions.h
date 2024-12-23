#pragma once

#include "common/data_chunk/data_chunk.h"
#include "function.h"

namespace kuzu {
namespace parser {
class ParsedExpression;
}

namespace processor {
struct ExecutionContext;
}

namespace function {

struct TableFuncBindInput;
struct TableFuncBindData;

struct TableFuncSharedState {
    virtual ~TableFuncSharedState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct TableFuncLocalState {
    virtual ~TableFuncLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct TableFuncInput {
    TableFuncBindData* bindData;
    TableFuncLocalState* localState;
    TableFuncSharedState* sharedState;
    processor::ExecutionContext* context;

    TableFuncInput() = default;
    TableFuncInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
        TableFuncSharedState* sharedState, processor::ExecutionContext* context)
        : bindData{bindData}, localState{localState}, sharedState{sharedState}, context{context} {}
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
    uint64_t queryID;

    explicit TableFunctionInitInput(TableFuncBindData* bindData, uint64_t queryID)
        : bindData{bindData}, queryID(queryID) {}

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
using table_func_finalize_t =
    std::function<void(processor::ExecutionContext*, TableFuncSharedState*)>;
using table_func_rewrite_t =
    std::function<std::string(main::ClientContext&, const TableFuncBindData& bindData)>;

struct KUZU_API TableFunction : public Function {
    table_func_t tableFunc = nullptr;
    table_func_bind_t bindFunc = nullptr;
    table_func_init_shared_t initSharedStateFunc = nullptr;
    table_func_init_local_t initLocalStateFunc = nullptr;
    table_func_can_parallel_t canParallelFunc = [] { return true; };
    table_func_progress_t progressFunc = [](TableFuncSharedState*) { return 0.0; };
    table_func_finalize_t finalizeFunc = [](auto, auto) {};
    table_func_rewrite_t rewriteFunc = nullptr;

    TableFunction() : Function{} {};
    TableFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
        : Function{name, inputTypes} {}

    std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    virtual std::unique_ptr<TableFunction> copy() const {
        return std::make_unique<TableFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu
