#pragma once

#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {

class BoundInQueryCall : public BoundReadingClause {
public:
    BoundInQueryCall(function::table_func_t tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, expression_vector outputExpressions)
        : BoundReadingClause{common::ClauseType::InQueryCall}, tableFunc{std::move(tableFunc)},
          bindData{std::move(bindData)}, outputExpressions{std::move(outputExpressions)} {}

    inline function::table_func_t getTableFunc() const { return tableFunc; }

    inline function::TableFuncBindData* getBindData() const { return bindData.get(); }

    inline expression_vector getOutputExpressions() const { return outputExpressions; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundInQueryCall>(tableFunc, bindData->copy(), outputExpressions);
    }

private:
    function::table_func_t tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    expression_vector outputExpressions;
};

} // namespace binder
} // namespace kuzu
