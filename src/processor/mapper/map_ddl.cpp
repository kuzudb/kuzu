#include "planner/logical_plan/logical_operator/logical_add_property.h"
#include "planner/logical_plan/logical_operator/logical_copy.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_property.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"
#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "planner/logical_plan/logical_operator/logical_rename_property.h"
#include "planner/logical_plan/logical_operator/logical_rename_table.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/copy/copy_node.h"
#include "processor/operator/copy/copy_rel.h"
#include "processor/operator/ddl/add_node_property.h"
#include "processor/operator/ddl/add_rel_property.h"
#include "processor/operator/ddl/create_node_table.h"
#include "processor/operator/ddl/create_rel_table.h"
#include "processor/operator/ddl/drop_property.h"
#include "processor/operator/ddl/drop_table.h"
#include "processor/operator/ddl/rename_property.h"
#include "processor/operator/ddl/rename_table.h"
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createNodeTable = (LogicalCreateNodeTable*)logicalOperator;
    return std::make_unique<CreateNodeTable>(catalog, createNodeTable->getTableName(),
        createNodeTable->getPropertyNameDataTypes(), createNodeTable->getPrimaryKeyIdx(),
        getDataPos(createNodeTable->getSchema(), createNodeTable->getOutputExpression()),
        getOperatorID(), createNodeTable->getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createRelTable = (LogicalCreateRelTable*)logicalOperator;
    return std::make_unique<CreateRelTable>(catalog, createRelTable->getTableName(),
        createRelTable->getPropertyNameDataTypes(), createRelTable->getRelMultiplicity(),
        createRelTable->getSrcTableID(), createRelTable->getDstTableID(),
        getDataPos(createRelTable->getSchema(), createRelTable->getOutputExpression()),
        getOperatorID(), createRelTable->getExpressionsForPrinting(),
        &storageManager.getRelsStore().getRelsStatistics());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyToPhysical(
    LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopy*)logicalOperator;
    auto tableName = catalog->getReadOnlyVersion()->getTableName(copy->getTableID());
    auto nodesStatistics = &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs();
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    if (catalog->getReadOnlyVersion()->containNodeTable(tableName)) {
        return std::make_unique<CopyNode>(catalog, copy->getCSVReaderConfig(), copy->getTableID(),
            storageManager.getWAL(), nodesStatistics, storageManager.getRelsStore(),
            getInputPos(copy), getDataPos(copy->getSchema(), copy->getOutputExpression()),
            std::move(prevOperator), getOperatorID(), copy->getExpressionsForPrinting());
    } else {
        return std::make_unique<CopyRel>(catalog, copy->getCSVReaderConfig(), copy->getTableID(),
            storageManager.getWAL(), nodesStatistics, relsStatistics, getInputPos(copy),
            getDataPos(copy->getSchema(), copy->getOutputExpression()), std::move(prevOperator),
            getOperatorID(), copy->getExpressionsForPrinting());
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return std::make_unique<DropTable>(catalog, dropTable->getTableID(),
        getDataPos(dropTable->getSchema(), dropTable->getOutputExpression()), getOperatorID(),
        dropTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRenameTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto renameTable = (LogicalRenameTable*)logicalOperator;
    return std::make_unique<RenameTable>(catalog, renameTable->getTableID(),
        renameTable->getNewName(),
        getDataPos(renameTable->getSchema(), renameTable->getOutputExpression()), getOperatorID(),
        renameTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAddPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto addProperty = (LogicalAddProperty*)logicalOperator;
    auto expressionEvaluator =
        expressionMapper.mapExpression(addProperty->getDefaultValue(), *addProperty->getSchema());
    if (catalog->getReadOnlyVersion()->containNodeTable(addProperty->getTableID())) {
        return std::make_unique<AddNodeProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType(),
            std::move(expressionEvaluator), storageManager,
            getDataPos(addProperty->getSchema(), addProperty->getOutputExpression()),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    } else {
        return std::make_unique<AddRelProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType(),
            std::move(expressionEvaluator), storageManager,
            getDataPos(addProperty->getSchema(), addProperty->getOutputExpression()),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropProperty = (LogicalDropProperty*)logicalOperator;
    return std::make_unique<DropProperty>(catalog, dropProperty->getTableID(),
        dropProperty->getPropertyID(),
        getDataPos(dropProperty->getSchema(), dropProperty->getOutputExpression()), getOperatorID(),
        dropProperty->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRenamePropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto renameProperty = (LogicalRenameProperty*)logicalOperator;
    return std::make_unique<RenameProperty>(catalog, renameProperty->getTableID(),
        renameProperty->getPropertyID(), renameProperty->getNewName(),
        getDataPos(renameProperty->getSchema(), renameProperty->getOutputExpression()),
        getOperatorID(), renameProperty->getExpressionsForPrinting());
}

DataPos PlanMapper::getInputPos(LogicalCopy* logicalCopy) {
    auto inSchema = logicalCopy->getChild(0)->getSchema();
    auto inExpression =
        ((planner::LogicalExpressionsScan*)logicalCopy->getChild(0).get())->getExpressions()[0];
    return getDataPos(inSchema, inExpression);
}

} // namespace processor
} // namespace kuzu
