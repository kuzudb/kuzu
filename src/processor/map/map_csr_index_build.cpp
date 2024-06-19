#include "binder/expression/property_expression.h"
#include "planner/operator/logical_csr_build.h"
#include "processor/operator/csr_index_build.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

static ScanRelTableInfo getRelTableScanInfo(const catalog::TableCatalogEntry& tableCatalogEntry,
    RelDataDirection direction, RelTable* relTable, const expression_vector& properties,
    const std::vector<storage::ColumnPredicateSet>& columnPredicates) {
    auto relTableID = tableCatalogEntry.getTableID();
    std::vector<column_id_t> columnIDs;
    for (auto& expr : properties) {
        auto& property = expr->constCast<PropertyExpression>();
        if (property.hasPropertyID(relTableID)) {
            auto propertyID = property.getPropertyID(relTableID);
            columnIDs.push_back(tableCatalogEntry.getColumnID(propertyID));
        } else {
            columnIDs.push_back(INVALID_COLUMN_ID);
        }
    }
    return ScanRelTableInfo(relTable, direction, std::move(columnIDs),
        copyVector(columnPredicates));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCSRIndexBuild(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCSRBuild = (LogicalCSRBuild*)logicalOperator;
    auto rel = logicalCSRBuild->getRel();
    auto child = mapOperator(logicalOperator->getChild(0).get());
    auto schema = logicalOperator->getSchema();
    auto resultSetDescriptor = std::make_unique<ResultSetDescriptor>(schema);
    auto boundNodeVectorPos =
        DataPos(schema->getExpressionPos(*logicalCSRBuild->getBoundNode()->getInternalID()));
    auto nbrNodeVectorPos =
        DataPos(schema->getExpressionPos(*logicalCSRBuild->getNbrNode()->getInternalID()));
    auto relIDVectorPos = DataPos(schema->getExpressionPos(*rel->getInternalIDProperty()));
    auto commonNodeTableID = logicalCSRBuild->getBoundNode()->getSingleTableID();
    auto commonEdgeTableID = rel->getSingleTableID();
    auto sharedState = std::make_shared<CSRIndexSharedState>();
    auto relDataDirection =
        ExtendDirectionUtil::getRelDataDirection(logicalCSRBuild->getDirection());
    auto relTableID = rel->getSingleTableID();
    auto entry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), relTableID);
    auto relTable = clientContext->getStorageManager()->getTable(relTableID)->ptrCast<RelTable>();
    auto scanRelInfo = getRelTableScanInfo(*entry, relDataDirection, relTable,
        rel->getPropertyExprs(), rel->getPropertyPredicates());
    return std::make_unique<CSRIndexBuild>(std::move(resultSetDescriptor), commonNodeTableID,
        commonEdgeTableID, boundNodeVectorPos, nbrNodeVectorPos, relIDVectorPos, sharedState,
        std::move(child), getOperatorID(), logicalCSRBuild->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
