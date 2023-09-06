#include "planner/logical_plan/ddl/logical_add_property.h"
#include "planner/logical_plan/ddl/logical_create_table.h"
#include "planner/logical_plan/ddl/logical_drop_property.h"
#include "planner/logical_plan/ddl/logical_drop_table.h"
#include "planner/logical_plan/ddl/logical_rename_property.h"
#include "planner/logical_plan/ddl/logical_rename_table.h"
#include "processor/operator/ddl/add_node_property.h"
#include "processor/operator/ddl/add_rel_property.h"
#include "processor/operator/ddl/create_node_table.h"
#include "processor/operator/ddl/create_rdf_graph.h"
#include "processor/operator/ddl/create_rel_table.h"
#include "processor/operator/ddl/create_rel_table_group.h"
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    switch (createTable->getInfo()->type) {
    case TableType::NODE: {
        return mapCreateNodeTable(logicalOperator);
    }
    case TableType::REL: {
        return mapCreateRelTable(logicalOperator);
    }
    case TableType::REL_GROUP: {
        return mapCreateRelTableGroup(logicalOperator);
    }
    case TableType::RDF: {
        return mapCreateRdfGraph(logicalOperator);
    }
    default:                                                         // LCOV_EXCL_START
        throw NotImplementedException("PlanMapper::mapCreateTable"); // LCOV_EXCL_STOP
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateNodeTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateNodeTable>(catalog, &storageManager,
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs(),
        createTable->getInfo()->copy(), getOutputPos(createTable), getOperatorID(),
        createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRelTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRelTable>(catalog,
        &storageManager.getRelsStore().getRelsStatistics(), createTable->getInfo()->copy(),
        getOutputPos(createTable), getOperatorID(), createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRelTableGroup(
    LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRelTableGroup>(catalog,
        &storageManager.getRelsStore().getRelsStatistics(), createTable->getInfo()->copy(),
        getOutputPos(createTable), getOperatorID(), createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRdfGraph(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRdfGraph>(catalog, &storageManager,
        &storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs(),
        &storageManager.getRelsStore().getRelsStatistics(), createTable->getInfo()->copy(),
        getOutputPos(createTable), getOperatorID(), createTable->getExpressionsForPrinting());
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
        ExpressionMapper::getEvaluator(addProperty->getDefaultValue(), addProperty->getSchema());
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(addProperty->getTableID());
    switch (tableSchema->getTableType()) {
    case TableType::NODE:
        return std::make_unique<AddNodeProperty>(catalog, addProperty->getTableID(),
            addProperty->getPropertyName(), addProperty->getDataType()->copy(),
            std::move(expressionEvaluator), storageManager, getOutputPos(addProperty),
            getOperatorID(), addProperty->getExpressionsForPrinting());
    case TableType::REL:
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
