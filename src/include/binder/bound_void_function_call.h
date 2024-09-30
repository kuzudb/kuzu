#pragma once

#include <utility>

#include "binder/bound_statement.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

class BoundVoidFunctionCall : public BoundStatement {
public:
    explicit BoundVoidFunctionCall(function::TableFunction function,
        std::unique_ptr<function::TableFuncBindData> bindData)
        : BoundStatement{common::StatementType::VOID_FUNCTION_CALL,
              BoundStatementResult::createEmptyResult()},
          function(std::move(function)), bindData(std::move(bindData)) {}

    const function::TableFunction& getFunction() const { return function; }
    const function::TableFuncBindData* getBindData() const { return bindData.get(); }

private:
    function::TableFunction function;
    std::unique_ptr<function::TableFuncBindData> bindData;
};

} // namespace binder
} // namespace kuzu
