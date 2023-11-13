#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_drop_table.h"
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
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
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
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateNodeTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateNodeTable>(catalog, &storageManager,
        createTable->getInfo()->copy(), getOutputPos(createTable), getOperatorID(),
        createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRelTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRelTable>(catalog, &storageManager,
        createTable->getInfo()->copy(), getOutputPos(createTable), getOperatorID(),
        createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRelTableGroup(
    LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRelTableGroup>(catalog, &storageManager,
        createTable->getInfo()->copy(), getOutputPos(createTable), getOperatorID(),
        createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateRdfGraph(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateRdfGraph>(catalog, &storageManager,
        createTable->getInfo()->copy(), getOutputPos(createTable), getOperatorID(),
        createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropTable(LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return std::make_unique<DropTable>(catalog, dropTable->getTableID(), getOutputPos(dropTable),
        getOperatorID(), dropTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAlter(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    switch (alter->getInfo()->alterType) {
    case AlterType::RENAME_TABLE: {
        return mapRenameTable(logicalOperator);
    }
    case AlterType::ADD_PROPERTY: {
        return mapAddProperty(logicalOperator);
    }
    case AlterType::DROP_PROPERTY: {
        return mapDropProperty(logicalOperator);
    }
    case AlterType::RENAME_PROPERTY: {
        return mapRenameProperty(logicalOperator);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRenameTable(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    auto info = alter->getInfo();
    auto extraInfo = reinterpret_cast<BoundExtraRenameTableInfo*>(info->extraInfo.get());
    return std::make_unique<RenameTable>(catalog, info->tableID, extraInfo->newName,
        getOutputPos(alter), getOperatorID(), alter->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAddProperty(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    auto info = alter->getInfo();
    auto extraInfo = reinterpret_cast<BoundExtraAddPropertyInfo*>(info->extraInfo.get());
    auto expressionEvaluator =
        ExpressionMapper::getEvaluator(extraInfo->defaultValue, alter->getSchema());
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(info->tableID);
    switch (tableSchema->getTableType()) {
    case TableType::NODE:
        return std::make_unique<AddNodeProperty>(catalog, info->tableID, extraInfo->propertyName,
            extraInfo->dataType->copy(), std::move(expressionEvaluator), storageManager,
            getOutputPos(alter), getOperatorID(), alter->getExpressionsForPrinting());
    case TableType::REL:
        return std::make_unique<AddRelProperty>(catalog, info->tableID, extraInfo->propertyName,
            extraInfo->dataType->copy(), std::move(expressionEvaluator), storageManager,
            getOutputPos(alter), getOperatorID(), alter->getExpressionsForPrinting());
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropProperty(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    auto info = alter->getInfo();
    auto extraInfo = reinterpret_cast<BoundExtraDropPropertyInfo*>(info->extraInfo.get());
    return std::make_unique<DropProperty>(catalog, info->tableID, extraInfo->propertyID,
        getOutputPos(alter), storageManager, getOperatorID(), alter->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRenameProperty(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    auto info = alter->getInfo();
    auto extraInfo = reinterpret_cast<BoundExtraRenamePropertyInfo*>(info->extraInfo.get());
    return std::make_unique<RenameProperty>(catalog, info->tableID, extraInfo->propertyID,
        extraInfo->newName, getOutputPos(alter), getOperatorID(),
        alter->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
