#include "planner/operator/persistent/logical_insert.h"
#include "processor/operator/persistent/insert.h"
#include "processor/plan_mapper.h"

using namespace kuzu::evaluator;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

static std::vector<DataPos> populateLhsVectorPositions(
    const std::vector<binder::expression_pair>& setItems, const Schema& outSchema) {
    std::vector<DataPos> result;
    for (auto& [lhs, rhs] : setItems) {
        if (outSchema.isExpressionInScope(*lhs)) {
            result.emplace_back(outSchema.getExpressionPos(*lhs));
        } else {
            result.emplace_back(INVALID_DATA_CHUNK_POS, INVALID_DATA_CHUNK_POS);
        }
    }
    return result;
}

std::unique_ptr<NodeInsertExecutor> PlanMapper::getNodeInsertExecutor(
    planner::LogicalInsertNodeInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) const {
    auto node = info->node;
    auto nodeTableID = node->getSingleTableID();
    auto table = storageManager.getNodeTable(nodeTableID);
    std::vector<RelTable*> relTablesToInit;
    for (auto& schema : catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(schema);
        if (relTableSchema->isSrcOrDstTable(nodeTableID)) {
            relTablesToInit.push_back(storageManager.getRelTable(schema->tableID));
        }
    }
    auto nodeIDPos = DataPos(outSchema.getExpressionPos(*node->getInternalID()));
    std::vector<DataPos> lhsVectorPositions = populateLhsVectorPositions(info->setItems, outSchema);
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    for (auto& [_, rhs] : info->setItems) {
        evaluators.push_back(ExpressionMapper::getEvaluator(rhs, &inSchema));
    }
    return std::make_unique<NodeInsertExecutor>(table, std::move(relTablesToInit), nodeIDPos,
        std::move(lhsVectorPositions), std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapInsertNode(LogicalOperator* logicalOperator) {
    auto logicalInsertNode = (LogicalInsertNode*)logicalOperator;
    auto outSchema = logicalInsertNode->getSchema();
    auto inSchema = logicalInsertNode->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeInsertExecutor>> executors;
    for (auto& info : logicalInsertNode->getInfosRef()) {
        executors.push_back(getNodeInsertExecutor(info.get(), *inSchema, *outSchema));
    }
    return std::make_unique<InsertNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalInsertNode->getExpressionsForPrinting());
}

std::unique_ptr<RelInsertExecutor> PlanMapper::getRelInsertExecutor(
    planner::LogicalInsertRelInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) {
    auto rel = info->rel;
    auto relTableID = rel->getSingleTableID();
    auto table = storageManager.getRelTable(relTableID);
    auto srcNode = rel->getSrcNode();
    auto dstNode = rel->getDstNode();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*srcNode->getInternalID()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*dstNode->getInternalID()));
    auto lhsVectorPositions = populateLhsVectorPositions(info->setItems, outSchema);
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    for (auto& [lhs, rhs] : info->setItems) {
        evaluators.push_back(ExpressionMapper::getEvaluator(rhs, &inSchema));
    }
    return std::make_unique<RelInsertExecutor>(*storageManager.getRelsStatistics(), table,
        srcNodePos, dstNodePos, std::move(lhsVectorPositions), std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapInsertRel(LogicalOperator* logicalOperator) {
    auto logicalInsertRel = (LogicalInsertRel*)logicalOperator;
    auto inSchema = logicalInsertRel->getChild(0)->getSchema();
    auto outSchema = logicalInsertRel->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelInsertExecutor>> executors;
    for (auto& info : logicalInsertRel->getInfosRef()) {
        executors.push_back(getRelInsertExecutor(info.get(), *inSchema, *outSchema));
    }
    return std::make_unique<InsertRel>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalInsertRel->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
