#pragma once

#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"

namespace kuzu {
namespace binder {

class BoundInQueryCall : public BoundReadingClause {
public:
    BoundInQueryCall(function::TableFunction* tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, expression_vector outExprs,
        std::shared_ptr<Expression> rowIdxExpr)
        : BoundReadingClause{common::ClauseType::IN_QUERY_CALL}, tableFunc{tableFunc},
          bindData{std::move(bindData)}, outExprs{std::move(outExprs)}, rowIdxExpr{std::move(
                                                                            rowIdxExpr)} {}

    inline function::TableFunction* getTableFunc() const { return tableFunc; }

    inline const function::TableFuncBindData* getBindData() const { return bindData.get(); }

    inline expression_vector getOutExprs() const { return outExprs; }

    inline std::shared_ptr<Expression> getRowIdxExpr() const { return rowIdxExpr; }

private:
    function::TableFunction* tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    expression_vector outExprs;
    std::shared_ptr<Expression> rowIdxExpr;
};

} // namespace binder
} // namespace kuzu
