#pragma once

#include "binder/bound_table_function_data.h"
#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

class BoundTableFunctionCall : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::TABLE_FUNCTION_CALL;

public:
    BoundTableFunctionCall(BoundTableFunction tableFunc, expression_vector columns)
        : BoundReadingClause{clauseType_}, tableFunc{std::move(tableFunc)},
          columns{std::move(columns)} {}

    function::TableFunction getTableFunc() const { return tableFunc.tableFunction; }
    const function::TableFuncBindData* getBindData() const { return tableFunc.bindData.get(); }
    std::shared_ptr<Expression> getOffset() const { return tableFunc.offset; }
    expression_vector getColumns() const { return columns; }

private:
    BoundTableFunction tableFunc;
    expression_vector columns;
};

} // namespace binder
} // namespace kuzu
