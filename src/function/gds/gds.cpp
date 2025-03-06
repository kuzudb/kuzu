#include "function/gds/gds.h"

#include "binder/binder.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/exception/binder.h"
#include "graph/on_disk_graph.h"
#include "main/client_context.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "planner/planner.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;
using namespace kuzu::planner;

namespace kuzu {
namespace function {

void GDSFuncSharedState::setGraphNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
    auto onDiskGraph = ku_dynamic_cast<graph::OnDiskGraph*>(graph.get());
    onDiskGraph->setNodeOffsetMask(maskMap.get());
    graphNodeMask = std::move(maskMap);
}

graph::GraphEntry GDSFunction::bindGraphEntry(main::ClientContext& context,
    const std::string& name) {
    if (!context.getGraphEntrySetUnsafe().hasGraph(name)) {
        throw BinderException(stringFormat("Cannot find graph {}.", name));
    }
    return context.getGraphEntrySetUnsafe().getEntry(name).copy();
}

std::shared_ptr<Expression> GDSFunction::bindNodeOutput(const TableFuncBindInput& bindInput,
    const std::vector<catalog::TableCatalogEntry*>& nodeEntries) {
    std::string nodeColumnName = NODE_COLUMN_NAME;
    if (!bindInput.yieldVariables.empty()) {
        nodeColumnName = bindColumnName(bindInput.yieldVariables[0], nodeColumnName);
    }
    auto node = bindInput.binder->createQueryNode(nodeColumnName, nodeEntries);
    bindInput.binder->addToScope(nodeColumnName, node);
    return node;
}

std::string GDSFunction::bindColumnName(const parser::YieldVariable& yieldVariable,
    std::string expressionName) {
    if (yieldVariable.name != expressionName) {
        throw common::BinderException{
            common::stringFormat("Unknown variable name: {}.", yieldVariable.name)};
    }
    return yieldVariable.hasAlias() ? yieldVariable.alias : expressionName;
}

std::unique_ptr<TableFuncSharedState> GDSFunction::initSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto bindData = input.bindData->constPtrCast<GDSBindData>();
    auto graph =
        std::make_unique<OnDiskGraph>(input.context->clientContext, bindData->graphEntry.copy());
    return std::make_unique<GDSFuncSharedState>(bindData->getResultTable(), std::move(graph));
}

std::vector<std::shared_ptr<LogicalOperator>> getNodeMaskPlanRoots(const GDSBindData& bindData,
    Planner* planner) {
    std::vector<std::shared_ptr<LogicalOperator>> nodeMaskPlanRoots;
    for (auto& nodeInfo : bindData.graphEntry.nodeInfos) {
        if (nodeInfo.predicate == nullptr) {
            continue;
        }
        auto p = planner->planNodeSemiMask(SemiMaskTargetType::GDS_GRAPH_NODE,
            nodeInfo.nodeOrRel->constCast<NodeExpression>(), nodeInfo.predicate);
        nodeMaskPlanRoots.push_back(p.getLastOperator());
    }
    return nodeMaskPlanRoots;
};

void GDSFunction::getLogicalPlan(Planner* planner, const BoundReadingClause& readingClause,
    std::shared_ptr<LogicalOperator>,
    const std::vector<std::unique_ptr<LogicalPlan>>& logicalPlans) {
    auto& call = readingClause.constCast<binder::BoundTableFunctionCall>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    planner::Planner::splitPredicates(call.getBindData()->columns, call.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);

    auto bindData = call.getBindData()->constPtrCast<GDSBindData>();
    auto maskRoots = getNodeMaskPlanRoots(*bindData, planner);
    for (auto& plan : logicalPlans) {
        auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(), bindData->copy());
        op->setNodeMaskRoots(maskRoots);
        op->computeFactorizedSchema();
        planner->planReadOp(std::move(op), predicatesToPush, *plan);
    }
    auto nodeOutput = bindData->nodeOutput;
    KU_ASSERT(nodeOutput != nullptr);
    auto properties = planner->getProperties(*nodeOutput);
    if (!properties.empty()) {
        auto& node = nodeOutput->constCast<NodeExpression>();
        auto scanPlan = LogicalPlan();
        planner->getCardinalityEstimator().addNodeIDDomAndStats(
            planner->clientContext->getTransaction(), *node.getInternalID(), node.getTableIDs());
        planner->appendScanNodeTable(node.getInternalID(), node.getTableIDs(), properties,
            scanPlan);
        expression_vector joinConditions;
        joinConditions.push_back(node.getInternalID());
        for (auto& plan : logicalPlans) {
            planner->appendHashJoin(joinConditions, JoinType::INNER, *plan, scanPlan, *plan);
        }
    }

    for (auto& plan : logicalPlans) {
        if (!predicatesToPull.empty()) {
            planner->appendFilters(predicatesToPull, *plan);
        }
    }
}

std::unique_ptr<PhysicalOperator> GDSFunction::getPhysicalPlan(PlanMapper* planMapper,
    const LogicalOperator* logicalOp) {
    auto logicalCall = logicalOp->constPtrCast<LogicalTableFunctionCall>();
    auto bindData = logicalCall->getBindData()->copy();
    auto columns = bindData->columns;
    auto tableSchema = PlanMapper::createFlatFTableSchema(columns, *logicalCall->getSchema());
    auto table = std::make_shared<FactorizedTable>(planMapper->clientContext->getMemoryManager(),
        tableSchema.copy());
    bindData->cast<GDSBindData>().setResultFTable(table);
    auto info = TableFunctionCallInfo();
    info.function = logicalCall->getTableFunc();
    info.bindData = std::move(bindData);
    auto initInput =
        TableFuncInitSharedStateInput(info.bindData.get(), planMapper->executionContext);
    auto sharedState = info.function.initSharedStateFunc(initInput);
    auto printInfo = std::make_unique<TableFunctionCallPrintInfo>("");
    auto call = std::make_unique<TableFunctionCall>(std::move(info), sharedState,
        planMapper->getOperatorID(), std::move(printInfo));
    if (!logicalCall->getNodeMaskRoots().empty()) {
        auto funcSharedState = sharedState->ptrCast<GDSFuncSharedState>();
        funcSharedState->setGraphNodeMask(std::make_unique<NodeOffsetMaskMap>());
        auto maskMap = funcSharedState->getGraphNodeMaskMap();
        planMapper->logicalOpToPhysicalOpMap.insert({logicalOp, call.get()});
        for (auto logicalRoot : logicalCall->getNodeMaskRoots()) {
            KU_ASSERT(logicalRoot->getNumChildren() == 1);
            auto child = logicalRoot->getChild(0);
            KU_ASSERT(child->getOperatorType() == LogicalOperatorType::SEMI_MASKER);
            auto logicalSemiMasker = child->ptrCast<LogicalSemiMasker>();
            logicalSemiMasker->addTarget(logicalOp);
            for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
                maskMap->addMask(tableID, planMapper->createSemiMask(tableID));
            }
            auto root = planMapper->mapOperator(logicalRoot.get());
            call->addChild(std::move(root));
        }
        planMapper->logicalOpToPhysicalOpMap.erase(logicalOp);
    }
    planMapper->logicalOpToPhysicalOpMap.insert({logicalOp, call.get()});
    physical_op_vector_t children;
    children.push_back(planMapper->createDummySink(logicalCall->getSchema(), std::move(call)));
    return planMapper->createFTableScanAligned(columns, logicalCall->getSchema(), table,
        DEFAULT_VECTOR_CAPACITY, std::move(children));
}

} // namespace function
} // namespace kuzu
