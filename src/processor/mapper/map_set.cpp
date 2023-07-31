#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/set.h"

using namespace kuzu::binder;
using namespace kuzu::planner;
using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

static std::unique_ptr<NodeSetExecutor> getNodeSetExecutor(storage::NodesStore* store,
    const LogicalSetPropertyInfo& info, const Schema& inSchema,
    std::unique_ptr<BaseExpressionEvaluator> evaluator) {
    auto node = (NodeExpression*)info.nodeOrRel.get();
    auto property = (PropertyExpression*)info.setItem.first.get();
    auto nodeIDPos = DataPos(inSchema.getExpressionPos(*node->getInternalIDProperty()));
    if (node->isMultiLabeled()) {
        std::unordered_map<common::table_id_t, storage::NodeColumn*> tableIDToColumn;
        for (auto tableID : node->getTableIDs()) {
            if (!property->hasPropertyID(tableID)) {
                continue;
            }
            auto propertyID = property->getPropertyID(tableID);
            auto column = store->getNodePropertyColumn(tableID, propertyID);
            tableIDToColumn.insert({tableID, column});
        }
        return std::make_unique<MultiLabelNodeSetExecutor>(
            std::move(tableIDToColumn), nodeIDPos, std::move(evaluator));
    } else {
        auto tableID = node->getSingleTableID();
        auto column = store->getNodePropertyColumn(tableID, property->getPropertyID(tableID));
        return std::make_unique<SingleLabelNodeSetExecutor>(
            column, nodeIDPos, std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetNodeProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto inSchema = logicalSetNodeProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto nodeStore = &storageManager.getNodesStore();
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
    for (auto& info : logicalSetNodeProperty.getInfosRef()) {
        auto evaluator = expressionMapper.mapExpression(info->setItem.second, *inSchema);
        executors.push_back(getNodeSetExecutor(nodeStore, *info, *inSchema, std::move(evaluator)));
    }
    return std::make_unique<SetNodeProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
}

static std::unique_ptr<RelSetExecutor> getRelSetExecutor(storage::RelsStore* store,
    const LogicalSetPropertyInfo& info, const Schema& inSchema,
    std::unique_ptr<BaseExpressionEvaluator> evaluator) {
    auto rel = (RelExpression*)info.nodeOrRel.get();
    auto property = (PropertyExpression*)info.setItem.first.get();
    auto srcNodePos =
        DataPos(inSchema.getExpressionPos(*rel->getSrcNode()->getInternalIDProperty()));
    auto dstNodePos =
        DataPos(inSchema.getExpressionPos(*rel->getDstNode()->getInternalIDProperty()));
    auto relIDPos = DataPos(inSchema.getExpressionPos(*rel->getInternalIDProperty()));
    if (rel->isMultiLabeled()) {
        std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::property_id_t>>
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
            srcNodePos, dstNodePos, relIDPos, std::move(evaluator));
    } else {
        auto tableID = rel->getSingleTableID();
        auto table = store->getRelTable(tableID);
        auto propertyID = property->getPropertyID(tableID);
        return std::make_unique<SingleLabelRelSetExecutor>(
            table, propertyID, srcNodePos, dstNodePos, relIDPos, std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetRelProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetRelProperty = (LogicalSetRelProperty&)*logicalOperator;
    auto inSchema = logicalSetRelProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto relStore = &storageManager.getRelsStore();
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
    for (auto& info : logicalSetRelProperty.getInfosRef()) {
        auto evaluator = expressionMapper.mapExpression(info->setItem.second, *inSchema);
        executors.push_back(getRelSetExecutor(relStore, *info, *inSchema, std::move(evaluator)));
    }
    return make_unique<SetRelProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetRelProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
