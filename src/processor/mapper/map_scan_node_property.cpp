#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto outSchema = scanProperty.getSchema();
    auto inSchema = scanProperty.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto node = scanProperty.getNode();
    auto inputNodeIDVectorPos = DataPos(inSchema->getExpressionPos(*node->getInternalIDProperty()));
    auto& nodeStore = storageManager.getNodesStore();
    vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    if (node->isMultiLabeled()) {
        unordered_map<table_id_t, vector<uint32_t>> tableIDToColumns;
        unordered_map<table_id_t, NodeTable*> tables;
        for (auto& tableID : node->getTableIDs()) {
            tables.insert({tableID, nodeStore.getNodeTable(tableID)});
            vector<uint32_t> columns;
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
        return make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, std::move(outVectorsPos),
            std::move(tables), std::move(tableIDToColumns), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = node->getSingleTableID();
        vector<uint32_t> columnIds;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            columnIds.push_back(property->getPropertyID(tableID));
        }
        return make_unique<ScanSingleNodeTable>(inputNodeIDVectorPos, std::move(outVectorsPos),
            nodeStore.getNodeTable(tableID), std::move(columnIds), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
