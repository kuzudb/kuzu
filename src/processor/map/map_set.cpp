#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "planner/operator/persistent/logical_set.h"
#include "processor/operator/persistent/set.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::evaluator;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static ExtraNodeSetInfo getExtraNodeSetInfo(main::ClientContext* context,
    common::table_id_t tableID, const PropertyExpression& propertyExpr) {
    auto storageManager = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto table = storageManager->getTable(tableID)->ptrCast<NodeTable>();
    auto columnID = INVALID_COLUMN_ID;
    if (propertyExpr.hasPropertyID(tableID)) {
        auto propertyID = propertyExpr.getPropertyID(tableID);
        auto entry = catalog->getTableCatalogEntry(context->getTx(), tableID);
        columnID = entry->getColumnID(propertyID);
    }
    return ExtraNodeSetInfo(table, columnID);
}

std::unique_ptr<NodeSetExecutor> PlanMapper::getNodeSetExecutor(const BoundSetPropertyInfo& info,
    const Schema& schema) const {
    auto& node = info.pattern->constCast<NodeExpression>();
    auto nodeIDPos = getDataPos(*node.getInternalID(), schema);
    auto& property = info.setItem.first->constCast<PropertyExpression>();
    auto propertyPos = DataPos::getInvalidPos();
    if (schema.isExpressionInScope(property)) {
        propertyPos = getDataPos(property, schema);
    }
    auto setInfo = NodeSetInfo(nodeIDPos, propertyPos);
    if (info.pkExpr != nullptr) {
        setInfo.pkPos = getDataPos(*info.pkExpr, schema);
    }
    auto exprMapper = ExpressionMapper(&schema);
    auto evaluator = exprMapper.getEvaluator(info.setItem.second);
    if (node.isMultiLabeled()) {
        common::table_id_map_t<ExtraNodeSetInfo> extraInfos;
        for (auto tableID : node.getTableIDs()) {
            auto extraInfo = getExtraNodeSetInfo(clientContext, tableID, property);
            if (extraInfo.columnID == INVALID_COLUMN_ID) {
                continue;
            }
            extraInfos.insert({tableID, std::move(extraInfo)});
        }
        return std::make_unique<MultiLabelNodeSetExecutor>(std::move(setInfo), std::move(evaluator),
            std::move(extraInfos));
    }
    auto extraInfo = getExtraNodeSetInfo(clientContext, node.getSingleTableID(), property);
    return std::make_unique<SingleLabelNodeSetExecutor>(std::move(setInfo), std::move(evaluator),
        std::move(extraInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetProperty(
    planner::LogicalOperator* logicalOperator) {
    auto set = logicalOperator->constPtrCast<LogicalSetProperty>();
    switch (set->getTableType()) {
    case TableType::NODE: {
        return mapSetNodeProperty(logicalOperator);
    }
    case TableType::REL: {
        return mapSetRelProperty(logicalOperator);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetNodeProperty(LogicalOperator* logicalOperator) {
    auto set = logicalOperator->constPtrCast<LogicalSetProperty>();
    auto inSchema = set->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
    for (auto& info : set->getInfos()) {
        executors.push_back(getNodeSetExecutor(info, *inSchema));
    }
    auto printInfo = std::make_unique<OPPrintInfo>(set->getExpressionsForPrinting());
    return std::make_unique<SetNodeProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<RelSetExecutor> PlanMapper::getRelSetExecutor(const BoundSetPropertyInfo& info,
    const Schema& schema) const {
    auto storageManager = clientContext->getStorageManager();
    auto catalog = clientContext->getCatalog();
    auto& rel = info.pattern->constCast<RelExpression>();
    auto srcNodePos = getDataPos(*rel.getSrcNode()->getInternalID(), schema);
    auto dstNodePos = getDataPos(*rel.getDstNode()->getInternalID(), schema);
    auto relIDPos = getDataPos(*rel.getInternalIDProperty(), schema);
    auto& property = info.setItem.first->constCast<PropertyExpression>();
    auto propertyPos = DataPos::getInvalidPos();
    if (schema.isExpressionInScope(property)) {
        propertyPos = getDataPos(property, schema);
    }
    auto exprMapper = ExpressionMapper(&schema);
    auto evaluator = exprMapper.getEvaluator(info.setItem.second);
    if (rel.isMultiLabeled()) {
        std::unordered_map<table_id_t, std::pair<RelTable*, column_id_t>> tableIDToTableAndColumnID;
        for (auto tableID : rel.getTableIDs()) {
            if (!property.hasPropertyID(tableID)) {
                continue;
            }
            auto table = storageManager->getTable(tableID)->ptrCast<RelTable>();
            auto propertyID = property.getPropertyID(tableID);
            auto columnID = catalog->getTableCatalogEntry(clientContext->getTx(), tableID)
                                ->getColumnID(propertyID);
            tableIDToTableAndColumnID.insert({tableID, std::make_pair(table, columnID)});
        }
        return std::make_unique<MultiLabelRelSetExecutor>(std::move(tableIDToTableAndColumnID),
            srcNodePos, dstNodePos, relIDPos, propertyPos, std::move(evaluator));
    }
    auto tableID = rel.getSingleTableID();
    auto table = storageManager->getTable(tableID)->ptrCast<RelTable>();
    auto columnID = common::INVALID_COLUMN_ID;
    if (property.hasPropertyID(tableID)) {
        auto propertyID = property.getPropertyID(tableID);
        columnID =
            catalog->getTableCatalogEntry(clientContext->getTx(), tableID)->getColumnID(propertyID);
    }
    return std::make_unique<SingleLabelRelSetExecutor>(table, columnID, srcNodePos, dstNodePos,
        relIDPos, propertyPos, std::move(evaluator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetRelProperty(LogicalOperator* logicalOperator) {
    auto set = logicalOperator->constPtrCast<LogicalSetProperty>();
    auto inSchema = set->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
    for (auto& info : set->getInfos()) {
        executors.push_back(getRelSetExecutor(info, *inSchema));
    }
    auto printInfo = std::make_unique<OPPrintInfo>(set->getExpressionsForPrinting());
    return std::make_unique<SetRelProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
