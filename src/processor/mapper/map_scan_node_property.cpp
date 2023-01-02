#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan_column/scan_node_properties.h"

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
        unordered_map<table_id_t, vector<Column*>> tableIDToColumns;
        for (auto& tableID : node->getTableIDs()) {
            vector<Column*> columns;
            for (auto& expression : scanProperty.getProperties()) {
                auto property = static_pointer_cast<PropertyExpression>(expression);
                if (!property->hasPropertyID(tableID)) {
                    columns.push_back(nullptr);
                } else {
                    columns.push_back(
                        nodeStore.getNodePropertyColumn(tableID, property->getPropertyID(tableID)));
                }
            }
            tableIDToColumns.insert({tableID, std::move(columns)});
        }
        return make_unique<ScanMultiNodeTableProperties>(inputNodeIDVectorPos,
            std::move(outVectorsPos), std::move(tableIDToColumns), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = node->getSingleTableID();
        vector<Column*> columns;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            columns.push_back(
                nodeStore.getNodePropertyColumn(tableID, property->getPropertyID(tableID)));
        }
        return make_unique<ScanSingleNodeTableProperties>(inputNodeIDVectorPos,
            std::move(outVectorsPos), std::move(columns), std::move(prevOperator), getOperatorID(),
            scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
