#include "function/table/table_function.h"

#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/exception/binder.h"
#include "function/table/bind_data.h"
#include "planner/planner.h"

namespace kuzu {
namespace function {

TableFuncMorsel TableFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curOffset <= maxOffset);
    if (curOffset == maxOffset) {
        return TableFuncMorsel::createInvalidMorsel();
    }
    const auto numValuesToOutput = std::min(common::DEFAULT_VECTOR_CAPACITY, maxOffset - curOffset);
    curOffset += numValuesToOutput;
    return {curOffset - numValuesToOutput, curOffset};
}

std::unique_ptr<TableFuncSharedState> TableFunction::initSharedState(
    const TableFunctionInitInput& input) {
    const auto bindData = common::ku_dynamic_cast<TableFuncBindData*>(input.bindData);
    return std::make_unique<TableFuncSharedState>(bindData->maxOffset);
}

std::unique_ptr<TableFuncLocalState> TableFunction::initEmptyLocalState(
    const TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

std::vector<std::string> TableFunction::extractYieldVariables(const std::vector<std::string>& names,
    const std::vector<parser::YieldVariable>& yieldVariables) {
    std::vector<std::string> variableNames;
    if (!yieldVariables.empty()) {
        if (yieldVariables.size() < names.size()) {
            throw common::BinderException{"Output variables must all appear in the yield clause."};
        }
        if (yieldVariables.size() > names.size()) {
            throw common::BinderException{"The number of variables in the yield clause exceeds the "
                                          "number of output variables of the table function."};
        }
        for (auto i = 0u; i < names.size(); i++) {
            if (names[i] != yieldVariables[i].name) {
                throw common::BinderException{common::stringFormat(
                    "Unknown table function output variable name: {}.", yieldVariables[i].name)};
            }
            auto variableName =
                yieldVariables[i].hasAlias() ? yieldVariables[i].alias : yieldVariables[i].name;
            variableNames.push_back(variableName);
        }
    } else {
        variableNames = names;
    }
    return variableNames;
}

void TableFunction::getLogicalPlan(const transaction::Transaction*, planner::Planner* planner,
    const binder::BoundReadingClause& readingClause,
    std::shared_ptr<planner::LogicalOperator> logicalOp,
    const std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans) {
    auto& call = readingClause.constCast<binder::BoundTableFunctionCall>();
    binder::expression_vector predicatesToPull;
    binder::expression_vector predicatesToPush;
    planner::Planner::splitPredicates(call.getColumns(), call.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    for (auto& plan : logicalPlans) {
        planner->planReadOp(logicalOp, predicatesToPush, *plan);
        if (!predicatesToPull.empty()) {
            planner->appendFilters(predicatesToPull, *plan);
        }
    }
}

} // namespace function
} // namespace kuzu
