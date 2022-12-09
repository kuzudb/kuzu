#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan_column/scan_node_properties.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto node = scanProperty.getNode();
    auto inputNodeIDVectorPos = mapperContext.getDataPos(node->getInternalIDPropertyName());
    auto& nodeStore = storageManager.getNodesStore();
    vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    if (node->getNumTableIDs() > 1) {
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
        auto tableID = node->getTableID();
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
