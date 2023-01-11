#include "planner/logical_plan/logical_operator/logical_copy_csv.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_property.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/copy_csv/copy_node_csv.h"
#include "processor/operator/copy_csv/copy_rel_csv.h"
#include "processor/operator/ddl/create_node_table.h"
#include "processor/operator/ddl/create_rel_table.h"
#include "processor/operator/ddl/drop_property.h"
#include "processor/operator/ddl/drop_table.h"

namespace kuzu {
namespace processor {

static DataPos getOutputPos(LogicalDDL* logicalDDL) {
    auto outSchema = logicalDDL->getSchema();
    auto outputExpression = logicalDDL->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createNodeTable = (LogicalCreateNodeTable*)logicalOperator;
    return make_unique<CreateNodeTable>(catalog, createNodeTable->getTableName(),
        createNodeTable->getPropertyNameDataTypes(), createNodeTable->getPrimaryKeyIdx(),
        getOutputPos(createNodeTable), getOperatorID(),
        createNodeTable->getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto createRelTable = (LogicalCreateRelTable*)logicalOperator;
    return make_unique<CreateRelTable>(catalog, createRelTable->getTableName(),
        createRelTable->getPropertyNameDataTypes(), createRelTable->getRelMultiplicity(),
        createRelTable->getSrcDstTableIDs(), getOutputPos(createRelTable), getOperatorID(),
        createRelTable->getExpressionsForPrinting(),
        &storageManager.getRelsStore().getRelsStatistics());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyCSVToPhysical(
    LogicalOperator* logicalOperator) {
    auto copyCSV = (LogicalCopyCSV*)logicalOperator;
    auto tableName = catalog->getReadOnlyVersion()->getTableName(copyCSV->getTableID());
    auto nodesStatistics = &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs();
    auto relsStatistics = &storageManager.getRelsStore().getRelsStatistics();
    if (catalog->getReadOnlyVersion()->containNodeTable(tableName)) {
        return make_unique<CopyNodeCSV>(catalog, copyCSV->getCSVDescription(),
            copyCSV->getTableID(), storageManager.getWAL(), nodesStatistics, getOperatorID(),
            copyCSV->getExpressionsForPrinting());
    } else {
        return make_unique<CopyRelCSV>(catalog, copyCSV->getCSVDescription(), copyCSV->getTableID(),
            storageManager.getWAL(), nodesStatistics, relsStatistics, getOperatorID(),
            copyCSV->getExpressionsForPrinting());
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropTableToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return make_unique<DropTable>(catalog, dropTable->getTableID(), storageManager,
        getOutputPos(dropTable), getOperatorID(), dropTable->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto dropProperty = (LogicalDropProperty*)logicalOperator;
    return make_unique<DropProperty>(catalog, dropProperty->getTableID(),
        dropProperty->getPropertyID(), storageManager, getOutputPos(dropProperty), getOperatorID(),
        dropProperty->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
