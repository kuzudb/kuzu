#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "planner/operator/persistent/logical_set.h"
#include "processor/operator/persistent/set.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

std::unique_ptr<NodeSetExecutor> PlanMapper::getNodeSetExecutor(
    planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema) {
    auto node = (NodeExpression*)info->nodeOrRel.get();
    auto nodeIDPos = DataPos(inSchema.getExpressionPos(*node->getInternalID()));
    auto property = (PropertyExpression*)info->setItem.first.get();
    auto propertyPos = DataPos(INVALID_DATA_CHUNK_POS, INVALID_VALUE_VECTOR_POS);
    if (inSchema.isExpressionInScope(*property)) {
        propertyPos = DataPos(inSchema.getExpressionPos(*property));
    }
    auto evaluator = ExpressionMapper::getEvaluator(info->setItem.second, &inSchema);
    if (node->isMultiLabeled()) {
        std::unordered_map<table_id_t, NodeSetInfo> tableIDToSetInfo;
        for (auto tableID : node->getTableIDs()) {
            if (!property->hasPropertyID(tableID)) {
                continue;
            }
            auto propertyID = property->getPropertyID(tableID);
            auto table = storageManager.getNodeTable(tableID);
            auto columnID =
                catalog->getReadOnlyVersion()->getTableSchema(tableID)->getColumnID(propertyID);
            tableIDToSetInfo.insert({tableID, NodeSetInfo{table, columnID}});
        }
        return std::make_unique<MultiLabelNodeSetExecutor>(
            std::move(tableIDToSetInfo), nodeIDPos, propertyPos, std::move(evaluator));
    } else {
        auto tableID = node->getSingleTableID();
        auto table = storageManager.getNodeTable(tableID);
        auto columnID = INVALID_COLUMN_ID;
        if (property->hasPropertyID(tableID)) {
            auto propertyID = property->getPropertyID(tableID);
            columnID =
                catalog->getReadOnlyVersion()->getTableSchema(tableID)->getColumnID(propertyID);
        }
        return std::make_unique<SingleLabelNodeSetExecutor>(
            NodeSetInfo{table, columnID}, nodeIDPos, propertyPos, std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetNodeProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto inSchema = logicalSetNodeProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
    for (auto& info : logicalSetNodeProperty.getInfosRef()) {
        executors.push_back(getNodeSetExecutor(info.get(), *inSchema));
    }
    return std::make_unique<SetNodeProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
}

std::unique_ptr<RelSetExecutor> PlanMapper::getRelSetExecutor(
    planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema) {
    auto rel = (RelExpression*)info->nodeOrRel.get();
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*rel->getSrcNode()->getInternalID()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*rel->getDstNode()->getInternalID()));
    auto relIDPos = DataPos(inSchema.getExpressionPos(*rel->getInternalIDProperty()));
    auto property = (PropertyExpression*)info->setItem.first.get();
    auto propertyPos = DataPos(INVALID_DATA_CHUNK_POS, INVALID_VALUE_VECTOR_POS);
    if (inSchema.isExpressionInScope(*property)) {
        propertyPos = DataPos(inSchema.getExpressionPos(*property));
    }
    auto evaluator = ExpressionMapper::getEvaluator(info->setItem.second, &inSchema);
    if (rel->isMultiLabeled()) {
        std::unordered_map<table_id_t, std::pair<storage::RelTable*, column_id_t>>
            tableIDToTableAndColumnID;
        for (auto tableID : rel->getTableIDs()) {
            if (!property->hasPropertyID(tableID)) {
                continue;
            }
            auto table = storageManager.getRelTable(tableID);
            auto propertyID = property->getPropertyID(tableID);
            auto columnID =
                catalog->getReadOnlyVersion()->getTableSchema(tableID)->getColumnID(propertyID);
            tableIDToTableAndColumnID.insert({tableID, std::make_pair(table, columnID)});
        }
        return std::make_unique<MultiLabelRelSetExecutor>(std::move(tableIDToTableAndColumnID),
            srcNodePos, dstNodePos, relIDPos, propertyPos, std::move(evaluator));
    } else {
        auto tableID = rel->getSingleTableID();
        auto table = storageManager.getRelTable(tableID);
        auto columnID = common::INVALID_COLUMN_ID;
        if (property->hasPropertyID(tableID)) {
            auto propertyID = property->getPropertyID(tableID);
            columnID =
                catalog->getReadOnlyVersion()->getTableSchema(tableID)->getColumnID(propertyID);
        }
        return std::make_unique<SingleLabelRelSetExecutor>(
            table, columnID, srcNodePos, dstNodePos, relIDPos, propertyPos, std::move(evaluator));
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetRelProperty(LogicalOperator* logicalOperator) {
    auto& logicalSetRelProperty = (LogicalSetRelProperty&)*logicalOperator;
    auto inSchema = logicalSetRelProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
    for (auto& info : logicalSetRelProperty.getInfosRef()) {
        executors.push_back(getRelSetExecutor(info.get(), *inSchema));
    }
    return make_unique<SetRelProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalSetRelProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
