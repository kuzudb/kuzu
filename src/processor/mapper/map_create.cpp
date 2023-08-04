#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/create.h"

using namespace kuzu::evaluator;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

static std::unique_ptr<NodeInsertExecutor> getNodeInsertExecutor(NodesStore* nodesStore,
    RelsStore* relsStore, const Catalog& catalog, const LogicalCreateNodeInfo& info,
    const Schema& outSchema, std::unique_ptr<BaseExpressionEvaluator> evaluator) {
    auto node = info.node;
    auto nodeTableID = node->getSingleTableID();
    auto table = nodesStore->getNodeTable(nodeTableID);
    std::vector<RelTable*> relTablesToInit;
    for (auto& schema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
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
        std::unique_ptr<BaseExpressionEvaluator> evaluator = nullptr;
        if (info->primaryKey != nullptr) {
            evaluator = expressionMapper.mapExpression(info->primaryKey, *inSchema);
        }
        executors.push_back(getNodeInsertExecutor(&storageManager.getNodesStore(),
            &storageManager.getRelsStore(), *catalog, *info, *outSchema, std::move(evaluator)));
    }
    return std::make_unique<CreateNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalCreateNode->getExpressionsForPrinting());
}

static std::unique_ptr<RelInsertExecutor> getRelInsertExecutor(RelsStore* relsStore,
    const LogicalCreateRelInfo& info, const Schema& inSchema,
    std::vector<std::unique_ptr<BaseExpressionEvaluator>> evaluators) {
    auto rel = info.rel;
    auto relTableID = rel->getSingleTableID();
    auto table = relsStore->getRelTable(relTableID);
    auto srcNode = rel->getSrcNode();
    auto dstNode = rel->getDstNode();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*srcNode->getInternalIDProperty()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*dstNode->getInternalIDProperty()));
    return std::make_unique<RelInsertExecutor>(
        relsStore->getRelsStatistics(), table, srcNodePos, dstNodePos, std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRel(LogicalOperator* logicalOperator) {
    auto logicalCreateRel = (LogicalCreateRel*)logicalOperator;
    auto inSchema = logicalCreateRel->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelInsertExecutor>> executors;
    for (auto& info : logicalCreateRel->getInfosRef()) {
        std::vector<std::unique_ptr<BaseExpressionEvaluator>> evaluators;
        for (auto& [lhs, rhs] : info->setItems) {
            evaluators.push_back(expressionMapper.mapExpression(rhs, *inSchema));
        }
        executors.push_back(getRelInsertExecutor(
            &storageManager.getRelsStore(), *info, *inSchema, std::move(evaluators)));
    }
    return std::make_unique<CreateRel>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalCreateRel->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
