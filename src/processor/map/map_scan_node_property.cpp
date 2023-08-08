#include "planner/logical_plan/scan/logical_scan_node_property.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/plan_mapper.h"

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
    auto node = scanProperty.getNode();
    auto inputNodeIDVectorPos = DataPos(inSchema->getExpressionPos(*node->getInternalIDProperty()));
    auto& nodeStore = storageManager.getNodesStore();
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    if (node->isMultiLabeled()) {
        std::unordered_map<table_id_t, std::vector<uint32_t>> tableIDToColumns;
        std::unordered_map<table_id_t, storage::NodeTable*> tables;
        for (auto& tableID : node->getTableIDs()) {
            tables.insert({tableID, nodeStore.getNodeTable(tableID)});
            std::vector<uint32_t> columns;
            for (auto& expression : scanProperty.getProperties()) {
                auto property = static_pointer_cast<PropertyExpression>(expression);
                if (!property->hasPropertyID(tableID)) {
                    columns.push_back(UINT32_MAX);
                } else {
                    columns.push_back(property->getPropertyID(tableID));
                }
            }
            tableIDToColumns.insert({tableID, std::move(columns)});
        }
        return std::make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, std::move(outVectorsPos),
            std::move(tables), std::move(tableIDToColumns), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = node->getSingleTableID();
        std::vector<uint32_t> columnIds;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            columnIds.push_back(property->getPropertyID(tableID));
        }
        return std::make_unique<ScanSingleNodeTable>(inputNodeIDVectorPos, std::move(outVectorsPos),
            nodeStore.getNodeTable(tableID), std::move(columnIds), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
