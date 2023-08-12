#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/persistent/logical_set.h"
#include "processor/operator/persistent/set.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

std::unique_ptr<NodeSetExecutor> PlanMapper::getNodeSetExecutor(storage::NodesStore* store,
    planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema) {
    auto node = (NodeExpression*)info->nodeOrRel.get();
    auto nodeIDPos = DataPos(inSchema.getExpressionPos(*node->getInternalIDProperty()));
    auto property = (PropertyExpression*)info->setItem.first.get();
    auto propertyPos = DataPos(INVALID_DATA_CHUNK_POS, INVALID_VALUE_VECTOR_POS);
    if (inSchema.isExpressionInScope(*property)) {
        propertyPos = DataPos(inSchema.getExpressionPos(*property));
    }
    auto evaluator = expressionMapper.mapExpression(info->setItem.second, inSchema);
    if (node->isMultiLabeled()) {
        std::unordered_map<table_id_t, NodeSetInfo> tableIDToSetInfo;
        for (auto tableID : node->getTableIDs()) {
            if (!property->hasPropertyID(tableID)) {
                continue;
            }
            auto propertyID = property->getPropertyID(tableID);
            auto table = store->getNodeTable(tableID);
            tableIDToSetInfo.insert({tableID, NodeSetInfo{table, propertyID}});
        }
        return std::make_unique<MultiLabelNodeSetExecutor>(
            std::move(tableIDToSetInfo), nodeIDPos, propertyPos, std::move(evaluator));
    } else {
        auto tableID = node->getSingleTableID();
        auto table = store->getNodeTable(tableID);
        return std::make_unique<SingleLabelNodeSetExecutor>(
            NodeSetInfo{table, property->getPropertyID(tableID)}, nodeIDPos, propertyPos,
            std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetNodeProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto inSchema = logicalSetNodeProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto nodeStore = &storageManager.getNodesStore();
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
    for (auto& info : logicalSetNodeProperty.getInfosRef()) {
        executors.push_back(getNodeSetExecutor(nodeStore, info.get(), *inSchema));
    }
    return std::make_unique<SetNodeProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
}

std::unique_ptr<RelSetExecutor> PlanMapper::getRelSetExecutor(storage::RelsStore* store,
    planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema) {
    auto rel = (RelExpression*)info->nodeOrRel.get();
    auto srcNodePos =
        DataPos(inSchema.getExpressionPos(*rel->getSrcNode()->getInternalIDProperty()));
    auto dstNodePos =
        DataPos(inSchema.getExpressionPos(*rel->getDstNode()->getInternalIDProperty()));
    auto relIDPos = DataPos(inSchema.getExpressionPos(*rel->getInternalIDProperty()));
    auto property = (PropertyExpression*)info->setItem.first.get();
    auto propertyPos = DataPos(INVALID_DATA_CHUNK_POS, INVALID_VALUE_VECTOR_POS);
    if (inSchema.isExpressionInScope(*property)) {
        propertyPos = DataPos(inSchema.getExpressionPos(*property));
    }
    auto evaluator = expressionMapper.mapExpression(info->setItem.second, inSchema);
    if (rel->isMultiLabeled()) {
        std::unordered_map<table_id_t, std::pair<storage::RelTable*, property_id_t>>
            tableIDToTableAndPropertyID;
        for (auto tableID : rel->getTableIDs()) {
            if (!property->hasPropertyID(tableID)) {
                continue;
            }
            auto table = store->getRelTable(tableID);
            auto propertyID = property->getPropertyID(tableID);
            tableIDToTableAndPropertyID.insert({tableID, std::make_pair(table, propertyID)});
        }
        return std::make_unique<MultiLabelRelSetExecutor>(std::move(tableIDToTableAndPropertyID),
            srcNodePos, dstNodePos, relIDPos, propertyPos, std::move(evaluator));
    } else {
        auto tableID = rel->getSingleTableID();
        auto table = store->getRelTable(tableID);
        auto propertyID = property->getPropertyID(tableID);
        return std::make_unique<SingleLabelRelSetExecutor>(
            table, propertyID, srcNodePos, dstNodePos, relIDPos, propertyPos, std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetRelProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetRelProperty = (LogicalSetRelProperty&)*logicalOperator;
    auto inSchema = logicalSetRelProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto relStore = &storageManager.getRelsStore();
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
    for (auto& info : logicalSetRelProperty.getInfosRef()) {
        executors.push_back(getRelSetExecutor(relStore, info.get(), *inSchema));
    }
    return make_unique<SetRelProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetRelProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
