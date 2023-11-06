#pragma once

#include "function.h"

namespace kuzu {
namespace catalog {
class CatalogContent;
} // namespace catalog
namespace common {
class ValueVector;
}
namespace main {
class ClientContext;
}

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

struct SharedTableFuncState {
    virtual ~SharedTableFuncState() = default;
};

struct TableFunctionInput {
    TableFuncBindData* bindData;
    SharedTableFuncState* sharedState;

    TableFunctionInput(TableFuncBindData* bindData, SharedTableFuncState* sharedState)
        : bindData{bindData}, sharedState{sharedState} {}
};

struct TableFunctionInitInput {
    TableFuncBindData* bindData;

    TableFunctionInitInput(TableFuncBindData* bindData) : bindData{bindData} {}

    virtual ~TableFunctionInitInput() = default;
};

typedef std::unique_ptr<TableFuncBindData> (*table_func_bind_t)(main::ClientContext* /*context*/,
    TableFuncBindInput /*input*/, catalog::CatalogContent* /*catalog*/);
typedef void (*table_func_t)(TableFunctionInput& data, std::vector<common::ValueVector*> output);
typedef std::unique_ptr<SharedTableFuncState> (*table_func_init_shared_t)(
    TableFunctionInitInput& input);

struct TableFunction : public Function {
    table_func_t tableFunc;
    table_func_bind_t bindFunc;
    table_func_init_shared_t initSharedStateFunc;

    TableFunction(std::string name, table_func_t tableFunc, table_func_bind_t bindFunc,
        table_func_init_shared_t initSharedFunc, std::vector<common::LogicalTypeID> inputTypes)
        : Function{FunctionType::TABLE, std::move(name), std::move(inputTypes)},
          tableFunc{std::move(tableFunc)}, bindFunc{std::move(bindFunc)}, initSharedStateFunc{
                                                                              initSharedFunc} {}

    inline std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }
};

} // namespace function
} // namespace kuzu
