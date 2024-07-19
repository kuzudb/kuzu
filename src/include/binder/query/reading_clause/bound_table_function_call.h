#pragma once

#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

class BoundTableFunctionCall : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::TABLE_FUNCTION_CALL;

public:
    BoundTableFunctionCall(function::TableFunction tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, std::shared_ptr<Expression> offset,
        expression_vector columns)
        : BoundReadingClause{clauseType_}, tableFunc{std::move(tableFunc)},
          bindData{std::move(bindData)}, offset{std::move(offset)}, columns{std::move(columns)} {}

    function::TableFunction getTableFunc() const { return tableFunc; }
    const function::TableFuncBindData* getBindData() const { return bindData.get(); }
    std::shared_ptr<Expression> getOffset() const { return offset; }
    expression_vector getColumns() const { return columns; }

private:
    function::TableFunction tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::shared_ptr<Expression> offset;
    expression_vector columns;
};

} // namespace binder
} // namespace kuzu
