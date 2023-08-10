#include "binder/expression/node_expression.h"
#include "planner/logical_plan/persistent/logical_create.h"
#include "processor/operator/persistent/insert.h"
#include "processor/plan_mapper.h"

using namespace kuzu::evaluator;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

std::unique_ptr<NodeInsertExecutor> PlanMapper::getNodeInsertExecutor(
    storage::NodesStore* nodesStore, storage::RelsStore* relsStore,
    planner::LogicalCreateNodeInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) {
    auto node = info->node;
    auto nodeTableID = node->getSingleTableID();
    auto table = nodesStore->getNodeTable(nodeTableID);
    std::unique_ptr<ExpressionEvaluator> evaluator = nullptr;
    if (info->primaryKey != nullptr) {
        evaluator = expressionMapper.mapExpression(info->primaryKey, inSchema);
    }
    std::vector<RelTable*> relTablesToInit;
    for (auto& schema : catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        if (schema->isSrcOrDstTable(nodeTableID)) {
            relTablesToInit.push_back(relsStore->getRelTable(schema->tableID));
        }
    }
    auto nodeIDPos = DataPos(outSchema.getExpressionPos(*node->getInternalIDProperty()));
    return std::make_unique<NodeInsertExecutor>(
        table, std::move(evaluator), std::move(relTablesToInit), nodeIDPos);
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateNode(LogicalOperator* logicalOperator) {
    auto logicalCreateNode = (LogicalCreateNode*)logicalOperator;
    auto outSchema = logicalCreateNode->getSchema();
    auto inSchema = logicalCreateNode->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeInsertExecutor>> executors;
    for (auto& info : logicalCreateNode->getInfosRef()) {
        executors.push_back(getNodeInsertExecutor(&storageManager.getNodesStore(),
            &storageManager.getRelsStore(), info.get(), *inSchema, *outSchema));
    }
    return std::make_unique<InsertNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalCreateNode->getExpressionsForPrinting());
}

std::unique_ptr<RelInsertExecutor> PlanMapper::getRelInsertExecutor(storage::RelsStore* relsStore,
    planner::LogicalCreateRelInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) {
    auto rel = info->rel;
    auto relTableID = rel->getSingleTableID();
    auto table = relsStore->getRelTable(relTableID);
    auto srcNode = rel->getSrcNode();
    auto dstNode = rel->getDstNode();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*srcNode->getInternalIDProperty()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*dstNode->getInternalIDProperty()));
    std::vector<DataPos> lhsVectorPositions;
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    for (auto& [lhs, rhs] : info->setItems) {
        if (outSchema.isExpressionInScope(*lhs)) {
            lhsVectorPositions.emplace_back(outSchema.getExpressionPos(*lhs));
        } else {
            lhsVectorPositions.emplace_back(INVALID_DATA_CHUNK_POS, INVALID_DATA_CHUNK_POS);
        }
        evaluators.push_back(expressionMapper.mapExpression(rhs, inSchema));
    }
    return std::make_unique<RelInsertExecutor>(relsStore->getRelsStatistics(), table, srcNodePos,
        dstNodePos, std::move(lhsVectorPositions), std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRel(LogicalOperator* logicalOperator) {
    auto logicalCreateRel = (LogicalCreateRel*)logicalOperator;
    auto inSchema = logicalCreateRel->getChild(0)->getSchema();
    auto outSchema = logicalCreateRel->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelInsertExecutor>> executors;
    for (auto& info : logicalCreateRel->getInfosRef()) {
        executors.push_back(getRelInsertExecutor(
            &storageManager.getRelsStore(), info.get(), *inSchema, *outSchema));
    }
    return std::make_unique<InsertRel>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalCreateRel->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
