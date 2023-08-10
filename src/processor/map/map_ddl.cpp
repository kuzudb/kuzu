#include "planner/logical_plan/ddl/logical_add_property.h"
#include "planner/logical_plan/ddl/logical_create_node_table.h"
#include "planner/logical_plan/ddl/logical_create_rel_table.h"
#include "planner/logical_plan/ddl/logical_drop_property.h"
#include "planner/logical_plan/ddl/logical_drop_table.h"
#include "planner/logical_plan/ddl/logical_rename_property.h"
#include "planner/logical_plan/ddl/logical_rename_table.h"
#include "processor/operator/ddl/add_node_property.h"
#include "processor/operator/ddl/add_rel_property.h"
#include "processor/operator/ddl/create_node_table.h"
#include "processor/operator/ddl/create_rel_table.h"
#include "processor/operator/ddl/drop_property.h"
#include "processor/operator/ddl/drop_table.h"
#include "processor/operator/ddl/rename_property.h"
#include "processor/operator/ddl/rename_table.h"
#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static DataPos getOutputPos(LogicalDDL* logicalDDL) {
    auto outSchema = logicalDDL->getSchema();
    auto outputExpression = logicalDDL->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateNodeTable(LogicalOperator* logicalOperator) {
    auto createNodeTable = (LogicalCreateNodeTable*)logicalOperator;
    return std::make_unique<CreateNodeTable>(catalog, createNodeTable->getTableName(),
        createNodeTable->getProperties(), createNodeTable->getPrimaryKeyIdx(), storageManager,
        getOutputPos(createNodeTable), getOperatorID(),
        createNodeTable->getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRelTable(LogicalOperator* logicalOperator) {
    auto createRelTable = (LogicalCreateRelTable*)logicalOperator;
    return std::make_unique<CreateRelTable>(catalog, createRelTable->getTableName(),
        createRelTable->getProperties(), createRelTable->getRelMultiplicity(),
        createRelTable->getSrcTableID(), createRelTable->getDstTableID(),
        getOutputPos(createRelTable), getOperatorID(), createRelTable->getExpressionsForPrinting(),
        &storageManager.getRelsStore().getRelsStatistics());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropTable(LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return std::make_unique<DropTable>(catalog, dropTable->getTableID(), getOutputPos(dropTable),
        getOperatorID(), dropTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRenameTable(LogicalOperator* logicalOperator) {
    auto renameTable = (LogicalRenameTable*)logicalOperator;
    return std::make_unique<RenameTable>(catalog, renameTable->getTableID(),
        renameTable->getNewName(), getOutputPos(renameTable), getOperatorID(),
        renameTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAddProperty(LogicalOperator* logicalOperator) {
    auto addProperty = (LogicalAddProperty*)logicalOperator;
    auto expressionEvaluator =
        expressionMapper.mapExpression(addProperty->getDefaultValue(), *addProperty->getSchema());
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(addProperty->getTableID());
    switch (tableSchema->getTableType()) {
    case catalog::TableType::NODE:
        return std::make_unique<AddNodeProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType()->copy(),
            std::move(expressionEvaluator), storageManager, getOutputPos(addProperty),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    case catalog::TableType::REL:
        return std::make_unique<AddRelProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType()->copy(),
            std::move(expressionEvaluator), storageManager, getOutputPos(addProperty),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    default:
        throw NotImplementedException{"PlanMapper::mapAddProperty"};
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropProperty(LogicalOperator* logicalOperator) {
    auto dropProperty = (LogicalDropProperty*)logicalOperator;
    return std::make_unique<DropProperty>(catalog, dropProperty->getTableID(),
        dropProperty->getPropertyID(), getOutputPos(dropProperty), getOperatorID(),
        dropProperty->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRenameProperty(LogicalOperator* logicalOperator) {
    auto renameProperty = (LogicalRenameProperty*)logicalOperator;
    return std::make_unique<RenameProperty>(catalog, renameProperty->getTableID(),
        renameProperty->getPropertyID(), renameProperty->getNewName(), getOutputPos(renameProperty),
        getOperatorID(), renameProperty->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
