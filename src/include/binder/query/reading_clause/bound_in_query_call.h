#pragma once

#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

class BoundInQueryCall : public BoundReadingClause {
public:
    BoundInQueryCall(function::TableFunction tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, expression_vector outExprs,
        std::shared_ptr<Expression> rowIdxExpr)
        : BoundReadingClause{common::ClauseType::IN_QUERY_CALL}, tableFunc{tableFunc},
          bindData{std::move(bindData)}, outExprs{std::move(outExprs)},
          rowIdxExpr{std::move(rowIdxExpr)} {}

    function::TableFunction getTableFunc() const { return tableFunc; }

    const function::TableFuncBindData* getBindData() const { return bindData.get(); }

    expression_vector getOutExprs() const { return outExprs; }

    std::shared_ptr<Expression> getRowIdxExpr() const { return rowIdxExpr; }

private:
    function::TableFunction tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    expression_vector outExprs;
    std::shared_ptr<Expression> rowIdxExpr;
};

} // namespace binder
} // namespace kuzu
