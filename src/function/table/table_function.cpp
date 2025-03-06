#include "function/table/table_function.h"

#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/exception/binder.h"
#include "function/table/bind_data.h"
#include "parser/query/reading_clause/yield_variable.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "planner/planner.h"
#include "processor/data_pos.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void TableFuncOutput::resetState() {
    dataChunk.state->getSelVectorUnsafe().setSelSize(0);
    dataChunk.resetAuxiliaryBuffer();
    for (auto i = 0u; i < dataChunk.getNumValueVectors(); i++) {
        dataChunk.getValueVectorMutable(i).setAllNonNull();
    }
}

void TableFuncOutput::setOutputSize(common::offset_t size) const {
    dataChunk.state->getSelVectorUnsafe().setToUnfiltered(size);
}

TableFunction::~TableFunction() = default;

std::unique_ptr<TableFuncLocalState> TableFunction::initEmptyLocalState(
    const TableFuncInitLocalStateInput&) {
    return std::make_unique<TableFuncLocalState>();
}

std::unique_ptr<TableFuncSharedState> TableFunction::initEmptySharedState(
    const kuzu::function::TableFuncInitSharedStateInput& /*input*/) {
    return std::make_unique<TableFuncSharedState>();
}

std::unique_ptr<TableFuncOutput> TableFunction::initSingleDataChunkScanOutput(
    const TableFuncInitOutputInput& input) {
    if (input.outColumnPositions.empty()) {
        return std::make_unique<TableFuncOutput>(DataChunk{});
    }
    auto state = input.resultSet.getDataChunk(input.outColumnPositions[0].dataChunkPos)->state;
    auto dataChunk = DataChunk(input.outColumnPositions.size(), state);
    for (auto i = 0u; i < input.outColumnPositions.size(); ++i) {
        dataChunk.insert(i, input.resultSet.getValueVector(input.outColumnPositions[i]));
    }
    return std::make_unique<TableFuncOutput>(std::move(dataChunk));
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

void TableFunction::getLogicalPlan(planner::Planner* planner,
    const binder::BoundReadingClause& readingClause,
    std::shared_ptr<planner::LogicalOperator> logicalOp,
    const std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans) {
    auto& call = readingClause.constCast<binder::BoundTableFunctionCall>();
    binder::expression_vector predicatesToPull;
    binder::expression_vector predicatesToPush;
    planner::Planner::splitPredicates(call.getBindData()->columns, call.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    for (auto& plan : logicalPlans) {
        planner->planReadOp(logicalOp, predicatesToPush, *plan);
    }
    if (!predicatesToPull.empty()) {
        for (auto& plan : logicalPlans) {
            planner->appendFilters(predicatesToPull, *plan);
        }
    }
}

std::unique_ptr<processor::PhysicalOperator> TableFunction::getPhysicalPlan(
    processor::PlanMapper* planMapper, const planner::LogicalOperator* logicalOp) {
    std::vector<processor::DataPos> outPosV;
    auto& call = logicalOp->constCast<planner::LogicalTableFunctionCall>();
    auto outSchema = call.getSchema();
    for (auto& expr : call.getBindData()->columns) {
        outPosV.emplace_back(planMapper->getDataPos(*expr, *outSchema));
    }
    auto info = processor::TableFunctionCallInfo();
    info.function = call.getTableFunc();
    info.bindData = call.getBindData()->copy();
    info.outPosV = outPosV;
    auto initInput =
        TableFuncInitSharedStateInput(info.bindData.get(), planMapper->executionContext);
    auto sharedState = info.function.initSharedStateFunc(initInput);
    if (!sharedState->semiMasks.getMasks().empty()) {
        for (const auto& logicalRoot : call.getNodeMaskRoots()) {
            auto logicalSemiMasker = planMapper->findSemiMaskerInPlan(logicalRoot.get());
            KU_ASSERT(logicalSemiMasker);
            logicalSemiMasker->addTarget(logicalOp);
        }
    }
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
