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
        std::unique_ptr<function::TableFuncBindData> bindData,
        std::shared_ptr<Expression> rowIdxExpr, expression_vector outExprs)
        : BoundReadingClause{clauseType_}, tableFunc{std::move(tableFunc)},
          bindData{std::move(bindData)}, rowIdxExpr{std::move(rowIdxExpr)},
          outExprs{std::move(outExprs)} {}

    function::TableFunction getTableFunc() const { return tableFunc; }
    const function::TableFuncBindData* getBindData() const { return bindData.get(); }
    std::shared_ptr<Expression> getRowIdxExpr() const { return rowIdxExpr; }
    expression_vector getOutExprs() const { return outExprs; }

private:
    function::TableFunction tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::shared_ptr<Expression> rowIdxExpr;
    expression_vector outExprs;
};

} // namespace binder
} // namespace kuzu
