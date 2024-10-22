#pragma once

#include "binder/bound_statement.h"
#include "bound_table_function_data.h"

namespace kuzu {
namespace binder {

class BoundStandaloneCallFunction : public BoundStatement {
public:
    explicit BoundStandaloneCallFunction(BoundTableFunction tableFunc)
        : BoundStatement{common::StatementType::STANDALONE_CALL_FUNCTION,
              BoundStatementResult::createEmptyResult()},
          tableFunc{std::move(tableFunc)} {}

    function::TableFunction getTableFunction() const { return tableFunc.tableFunction; }

    function::TableFuncBindData* getBindData() const { return tableFunc.bindData.get(); }

    std::shared_ptr<Expression> getOffset() const { return tableFunc.offset; }

private:
    BoundTableFunction tableFunc;
};

} // namespace binder
} // namespace kuzu
