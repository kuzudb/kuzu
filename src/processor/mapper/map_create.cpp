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

std::unique_ptr<NodeTableInsertManager> PlanMapper::createNodeTableInsertManager(
    planner::LogicalCreateNodeInfo* info, const planner::Schema& inSchema,
    const planner::Schema& outSchema) {
    auto& nodesStore = storageManager.getNodesStore();
    auto catalogContent = catalog->getReadOnlyVersion();
    auto nodeTableID = info->node->getSingleTableID();
    auto table = nodesStore.getNodeTable(nodeTableID);
    std::unique_ptr<BaseExpressionEvaluator> evaluator = nullptr;
    if (info->primaryKey != nullptr) {
        evaluator = expressionMapper.mapExpression(info->primaryKey, inSchema);
    }
    std::vector<RelTable*> relTablesToInit;
    for (auto& schema : catalogContent->getRelTableSchemas()) {
        if (schema->isSrcOrDstTable(nodeTableID)) {
            relTablesToInit.push_back(storageManager.getRelsStore().getRelTable(schema->tableID));
        }
    }
    auto outNodePos = DataPos(outSchema.getExpressionPos(*info->node->getInternalIDProperty()));
    return std::make_unique<NodeTableInsertManager>(
        table, std::move(evaluator), std::move(relTablesToInit), outNodePos);
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateNode(LogicalOperator* logicalOperator) {
    auto logicalCreateNode = (LogicalCreateNode*)logicalOperator;
    auto outSchema = logicalCreateNode->getSchema();
    auto inSchema = logicalCreateNode->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeTableInsertManager>> managers;
    for (auto& info : logicalCreateNode->getInfosRef()) {
        managers.push_back(createNodeTableInsertManager(info.get(), *inSchema, *outSchema));
    }
    return std::make_unique<CreateNode>(std::move(managers), std::move(prevOperator),
        getOperatorID(), logicalCreateNode->getExpressionsForPrinting());
}

std::unique_ptr<RelTableInsertManager> PlanMapper::createRelTableInsertManager(
    planner::LogicalCreateRelInfo* info, const planner::Schema& inSchema) {
    auto& relStore = storageManager.getRelsStore();
    auto relTableID = info->rel->getSingleTableID();
    auto table = relStore.getRelTable(relTableID);
    auto srcNode = info->rel->getSrcNode();
    auto dstNode = info->rel->getDstNode();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*srcNode->getInternalIDProperty()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*dstNode->getInternalIDProperty()));
    auto srcNodeTableID = srcNode->getSingleTableID();
    auto dstNodeTableID = dstNode->getSingleTableID();
    std::vector<std::unique_ptr<BaseExpressionEvaluator>> evaluators;
    for (auto& setItem : info->setItems) {
        evaluators.push_back(expressionMapper.mapExpression(setItem.second, inSchema));
    }
    return std::make_unique<RelTableInsertManager>(relStore.getRelsStatistics(), table, srcNodePos,
        dstNodePos, srcNodeTableID, dstNodeTableID, std::move(evaluators));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRel(LogicalOperator* logicalOperator) {
    auto logicalCreateRel = (LogicalCreateRel*)logicalOperator;
    auto inSchema = logicalCreateRel->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelTableInsertManager>> managers;
    for (auto& info : logicalCreateRel->getInfosRef()) {
        managers.push_back(createRelTableInsertManager(info.get(), *inSchema));
    }
    return std::make_unique<CreateRel>(std::move(managers), std::move(prevOperator),
        getOperatorID(), logicalCreateRel->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
