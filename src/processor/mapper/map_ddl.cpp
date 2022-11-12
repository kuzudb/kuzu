#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_copy_csv.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_node_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_rel_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_drop_table.h"
#include "src/processor/operator/copy_csv/include/copy_node_csv.h"
#include "src/processor/operator/copy_csv/include/copy_rel_csv.h"
#include "src/processor/operator/ddl/include/create_node_table.h"
#include "src/processor/operator/ddl/include/create_rel_table.h"
#include "src/processor/operator/ddl/include/drop_table.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeTableToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto createNodeTable = (LogicalCreateNodeTable*)logicalOperator;
    return make_unique<CreateNodeTable>(catalog, createNodeTable->getTableName(),
        createNodeTable->getPropertyNameDataTypes(), createNodeTable->getPrimaryKeyIdx(),
        getOperatorID(), createNodeTable->getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelTableToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto createRelTable = (LogicalCreateRelTable*)logicalOperator;
    return make_unique<CreateRelTable>(catalog, createRelTable->getTableName(),
        createRelTable->getPropertyNameDataTypes(), createRelTable->getRelMultiplicity(),
        createRelTable->getSrcDstTableIDs(), getOperatorID(),
        createRelTable->getExpressionsForPrinting(),
        &storageManager.getRelsStore().getRelsStatistics());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyCSVToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto copyCSV = (LogicalCopyCSV*)logicalOperator;
    if (copyCSV->getTableSchema().isNodeTable) {
        return make_unique<CopyNodeCSV>(catalog, copyCSV->getCSVDescription(),
            copyCSV->getTableSchema(), storageManager.getWAL(), getOperatorID(),
            copyCSV->getExpressionsForPrinting(), &storageManager.getNodesStore());
    } else {
        return make_unique<CopyRelCSV>(catalog, copyCSV->getCSVDescription(),
            copyCSV->getTableSchema(), storageManager.getWAL(),
            &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs(), getOperatorID(),
            copyCSV->getExpressionsForPrinting(),
            &storageManager.getRelsStore().getRelsStatistics());
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDropTableToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return make_unique<DropTable>(catalog, dropTable->getTableSchema(), storageManager,
        getOperatorID(), dropTable->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
