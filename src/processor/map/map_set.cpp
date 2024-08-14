#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "common/exception/binder.h"
#include "planner/operator/persistent/logical_set.h"
#include "processor/operator/persistent/set.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::planner;
using namespace kuzu::evaluator;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static column_id_t getColumnID(const catalog::TableCatalogEntry& entry,
    const PropertyExpression& propertyExpr) {
    auto columnID = INVALID_COLUMN_ID;
    if (propertyExpr.hasProperty(entry.getTableID())) {
        columnID = entry.getColumnID(propertyExpr.getPropertyName());
    }
    return columnID;
}

NodeTableSetInfo PlanMapper::getNodeTableSetInfo(const TableCatalogEntry& entry,
    const Expression& expr) const {
    auto storageManager = clientContext->getStorageManager();
    auto table = storageManager->getTable(entry.getTableID())->ptrCast<NodeTable>();
    auto columnID = getColumnID(entry, expr.constCast<PropertyExpression>());
    return NodeTableSetInfo(table, columnID);
}

RelTableSetInfo PlanMapper::getRelTableSetInfo(const TableCatalogEntry& entry,
    const Expression& expr) const {
    auto storageManager = clientContext->getStorageManager();
    auto table = storageManager->getTable(entry.getTableID())->ptrCast<RelTable>();
    auto columnID = getColumnID(entry, expr.constCast<PropertyExpression>());
    return RelTableSetInfo(table, columnID);
}

std::unique_ptr<NodeSetExecutor> PlanMapper::getNodeSetExecutor(
    const BoundSetPropertyInfo& boundInfo, const Schema& schema) const {
    auto& node = boundInfo.pattern->constCast<NodeExpression>();
    auto nodeIDPos = getDataPos(*node.getInternalID(), schema);
    auto& property = boundInfo.column->constCast<PropertyExpression>();
    auto columnVectorPos = DataPos::getInvalidPos();
    if (schema.isExpressionInScope(property)) {
        columnVectorPos = getDataPos(property, schema);
    }
    auto pkVectorPos = DataPos::getInvalidPos();
    if (boundInfo.updatePk) {
        pkVectorPos = getDataPos(*boundInfo.column, schema);
    }
    auto exprMapper = ExpressionMapper(&schema);
    auto evaluator = exprMapper.getEvaluator(boundInfo.columnData);
    auto setInfo = NodeSetInfo(nodeIDPos, columnVectorPos, pkVectorPos, std::move(evaluator));
    if (node.isMultiLabeled()) {
        common::table_id_map_t<NodeTableSetInfo> tableInfos;
        for (auto entry : node.getEntries()) {
            auto tableID = entry->getTableID();
            if (boundInfo.updatePk && !property.isPrimaryKey(tableID)) {
                throw BinderException(stringFormat(
                    "Update primary key column {} for multiple tables is not supported.",
                    property.toString()));
            }
            auto tableInfo = getNodeTableSetInfo(*entry, property);
            if (tableInfo.columnID == INVALID_COLUMN_ID) {
                continue;
            }
            tableInfos.insert({tableID, std::move(tableInfo)});
        }
        return std::make_unique<MultiLabelNodeSetExecutor>(std::move(setInfo),
            std::move(tableInfos));
    }
    auto tableInfo = getNodeTableSetInfo(*node.getSingleEntry(), property);
    return std::make_unique<SingleLabelNodeSetExecutor>(std::move(setInfo), std::move(tableInfo));
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
    std::vector<binder::expression_pair> expressions;
    for (auto& info : set->getInfos()) {
        expressions.emplace_back(info.column, info.columnData);
    }
    auto printInfo = std::make_unique<SetPropertyPrintInfo>(expressions);
    return std::make_unique<SetNodeProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<RelSetExecutor> PlanMapper::getRelSetExecutor(const BoundSetPropertyInfo& boundInfo,
    const Schema& schema) const {
    auto& rel = boundInfo.pattern->constCast<RelExpression>();
    auto srcNodeIDPos = getDataPos(*rel.getSrcNode()->getInternalID(), schema);
    auto dstNodeIDPos = getDataPos(*rel.getDstNode()->getInternalID(), schema);
    auto relIDPos = getDataPos(*rel.getInternalIDProperty(), schema);
    auto& property = boundInfo.column->constCast<PropertyExpression>();
    auto columnVectorPos = DataPos::getInvalidPos();
    if (schema.isExpressionInScope(property)) {
        columnVectorPos = getDataPos(property, schema);
    }
    auto exprMapper = ExpressionMapper(&schema);
    auto evaluator = exprMapper.getEvaluator(boundInfo.columnData);
    auto info =
        RelSetInfo(srcNodeIDPos, dstNodeIDPos, relIDPos, columnVectorPos, std::move(evaluator));
    if (rel.isMultiLabeled()) {
        common::table_id_map_t<RelTableSetInfo> tableInfos;
        for (auto entry : rel.getEntries()) {
            auto tableInfo = getRelTableSetInfo(*entry, property);
            if (tableInfo.columnID == INVALID_COLUMN_ID) {
                continue;
            }
            tableInfos.insert({entry->getTableID(), std::move(tableInfo)});
        }
        return std::make_unique<MultiLabelRelSetExecutor>(std::move(info), std::move(tableInfos));
    }
    auto tableInfo = getRelTableSetInfo(*rel.getSingleEntry(), property);
    return std::make_unique<SingleLabelRelSetExecutor>(std::move(info), std::move(tableInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSetRelProperty(LogicalOperator* logicalOperator) {
    auto set = logicalOperator->constPtrCast<LogicalSetProperty>();
    auto inSchema = set->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
    for (auto& info : set->getInfos()) {
        executors.push_back(getRelSetExecutor(info, *inSchema));
    }
    std::vector<binder::expression_pair> expressions;
    for (auto& info : set->getInfos()) {
        expressions.emplace_back(info.column, info.columnData);
    }
    auto printInfo = std::make_unique<SetPropertyPrintInfo>(expressions);
    return std::make_unique<SetRelProperty>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
