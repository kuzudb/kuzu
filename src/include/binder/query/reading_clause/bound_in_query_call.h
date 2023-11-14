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
        std::unique_ptr<function::TableFuncBindData> bindData, expression_vector outputExpressions,
        std::shared_ptr<Expression> rowIdxExpression)
        : BoundReadingClause{common::ClauseType::IN_QUERY_CALL}, tableFunc{std::move(tableFunc)},
          bindData{std::move(bindData)}, outputExpressions{std::move(outputExpressions)},
          rowIdxExpression{std::move(rowIdxExpression)} {}

    inline function::TableFunction* getTableFunc() const { return tableFunc; }

    inline function::TableFuncBindData* getBindData() const { return bindData.get(); }

    inline expression_vector getOutputExpressions() const { return outputExpressions; }

    inline std::shared_ptr<Expression> getRowIdxExpression() const { return rowIdxExpression; }

    inline void setWherePredicate(std::shared_ptr<Expression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline std::shared_ptr<Expression> getWherePredicate() const { return wherePredicate; }
    inline expression_vector getPredicatesSplitOnAnd() const {
        return hasWherePredicate() ? wherePredicate->splitOnAND() : expression_vector{};
    }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundInQueryCall>(
            tableFunc, bindData->copy(), outputExpressions, rowIdxExpression);
    }

private:
    function::TableFunction* tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    expression_vector outputExpressions;
    std::shared_ptr<Expression> rowIdxExpression;
    std::shared_ptr<Expression> wherePredicate;
};

} // namespace binder
} // namespace kuzu
