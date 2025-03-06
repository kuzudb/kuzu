#include "function/table/table_function.h"

#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/exception/binder.h"
#include "common/system_config.h"
#include "function/table/bind_data.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"
#include "processor/data_pos.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace function {

TableFuncSharedState::TableFuncSharedState()
    : maxOffset{0}, curOffset{0}, maxMorselSize{common::DEFAULT_VECTOR_CAPACITY} {}

TableFuncSharedState::TableFuncSharedState(common::offset_t maxOffset)
    : maxOffset{maxOffset}, curOffset{0}, maxMorselSize{common::DEFAULT_VECTOR_CAPACITY} {}

TableFuncSharedState::TableFuncSharedState(common::offset_t maxOffset,
    common::offset_t maxMorselSize)
    : maxOffset{maxOffset}, curOffset{0}, maxMorselSize{maxMorselSize} {}

TableFuncMorsel TableFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curOffset <= maxOffset);
    if (curOffset == maxOffset) {
        return TableFuncMorsel::createInvalidMorsel();
    }
    const auto numValuesToOutput = std::min(maxMorselSize, maxOffset - curOffset);
    curOffset += numValuesToOutput;
    return {curOffset - numValuesToOutput, curOffset};
}

TableFunction::~TableFunction() = default;

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

std::unique_ptr<processor::PhysicalOperator> TableFunction::getPhysicalPlan(
    const main::ClientContext* clientContext, processor::PlanMapper* planMapper,
    const planner::LogicalOperator* logicalOp) {
    std::vector<processor::DataPos> outPosV;
    auto& call = logicalOp->constCast<planner::LogicalTableFunctionCall>();
    auto outSchema = call.getSchema();
    for (auto& expr : call.getColumns()) {
        outPosV.emplace_back(planMapper->getDataPos(*expr, *outSchema));
    }
    auto info = processor::TableFunctionCallInfo();
    info.function = call.getTableFunc();
    info.bindData = call.getBindData()->copy();
    info.outPosV = outPosV;
    KU_ASSERT(info.outPosV.size() >= info.bindData->numWarningDataColumns);
    info.outputType = outPosV.empty() ? processor::TableScanOutputType::EMPTY :
                                        processor::TableScanOutputType::SINGLE_DATA_CHUNK;
    auto sharedState = std::make_shared<processor::TableFunctionCallSharedState>();
    TableFunctionInitInput tableFunctionInitInput{info.bindData.get(), 0 /* queryID */,
        *clientContext};
    sharedState->funcState = info.function.initSharedStateFunc(tableFunctionInitInput);
    auto printInfo =
        std::make_unique<processor::TableFunctionCallPrintInfo>(call.getTableFunc().name);
    return std::make_unique<processor::TableFunctionCall>(std::move(info), sharedState,
        planMapper->getOperatorID(), std::move(printInfo));
}

common::offset_t TableFunction::emptyTableFunc(const TableFuncInput&, TableFuncOutput&) {
    // DO NOTHING.
    return 0;
}

} // namespace function
} // namespace kuzu
