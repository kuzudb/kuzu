#pragma once

#include "binder/bound_statement.h"
#include "function/table/bind_data.h"

namespace kuzu {
namespace binder {

class BoundStandaloneCallFunction : public BoundStatement {
public:
    BoundStandaloneCallFunction(function::TableFunction tableFunction,
        std::unique_ptr<function::TableFuncBindData> bindData, std::shared_ptr<Expression> offset)
        : BoundStatement{common::StatementType::STANDALONE_CALL_FUNCTION,
              BoundStatementResult::createEmptyResult()},
          tableFunction{std::move(tableFunction)}, bindData{std::move(bindData)},
          offset{std::move(offset)} {}

    function::TableFunction getTableFunction() const { return tableFunction; }

    function::TableFuncBindData* getBindData() const { return bindData.get(); }

    std::shared_ptr<Expression> getOffset() const { return offset; }

private:
    function::TableFunction tableFunction;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::shared_ptr<binder::Expression> offset;
};

} // namespace binder
} // namespace kuzu
