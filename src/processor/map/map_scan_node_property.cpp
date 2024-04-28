#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_property.h"
#include "processor/operator/scan/scan_multi_node_tables.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanNodeProperty(
    LogicalOperator* logicalOperator) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto outSchema = scanProperty.getSchema();
    auto inSchema = scanProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto inputNodeIDVectorPos = DataPos(inSchema->getExpressionPos(*scanProperty.getNodeID()));
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto tableIDs = scanProperty.getTableIDs();
    if (tableIDs.size() > 1) {
        std::unordered_map<table_id_t, std::unique_ptr<ScanNodeTableInfo>> tables;
        for (auto& tableID : tableIDs) {
            std::vector<uint32_t> columns;
            for (auto& expression : scanProperty.getProperties()) {
                auto property = static_pointer_cast<PropertyExpression>(expression);
                if (!property->hasPropertyID(tableID)) {
                    columns.push_back(UINT32_MAX);
                } else {
                    columns.push_back(clientContext->getCatalog()
                                          ->getTableCatalogEntry(clientContext->getTx(), tableID)
                                          ->getColumnID(property->getPropertyID(tableID)));
                }
            }
            tables.insert({tableID, std::make_unique<ScanNodeTableInfo>(
                                        ku_dynamic_cast<storage::Table*, storage::NodeTable*>(
                                            clientContext->getStorageManager()->getTable(tableID)),
                                        std::move(columns))});
        }
        return std::make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, std::move(outVectorsPos),
            std::move(tables), std::move(prevOperator), getOperatorID(),
            scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = tableIDs[0];
        auto tableSchema =
            clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), tableID);
        std::vector<column_id_t> columnIDs;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            if (property->hasPropertyID(tableID)) {
                columnIDs.push_back(tableSchema->getColumnID(property->getPropertyID(tableID)));
            } else {
                columnIDs.push_back(UINT32_MAX);
            }
        }
        auto info = std::make_unique<ScanNodeTableInfo>(
            ku_dynamic_cast<storage::Table*, storage::NodeTable*>(
                clientContext->getStorageManager()->getTable(tableID)),
            std::move(columnIDs));
        return std::make_unique<ScanNodeTable>(std::move(info), inputNodeIDVectorPos,
            std::move(outVectorsPos), std::move(prevOperator), getOperatorID(),
            scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
