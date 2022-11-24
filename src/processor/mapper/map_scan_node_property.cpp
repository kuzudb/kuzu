#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/processor/operator/scan_column/include/scan_structured_property.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto node = scanProperty.getNode();
    auto inputNodeIDVectorPos = mapperContext.getDataPos(node->getIDProperty());
    auto& nodeStore = storageManager.getNodesStore();
    vector<DataPos> outVectorsPos;
    vector<DataType> outDataTypes;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        outDataTypes.push_back(expression->getDataType());
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    if (node->getNumTableIDs() > 1) {
        vector<unordered_map<table_id_t, Column*>> tableIDToColumns;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            unordered_map<table_id_t, Column*> tableIDToColumn;
            for (auto tableID : node->getTableIDs()) {
                if (!property->hasPropertyID(tableID)) {
                    continue;
                }
                tableIDToColumn.insert({tableID,
                    nodeStore.getNodePropertyColumn(tableID, property->getPropertyID(tableID))});
            }
            tableIDToColumns.push_back(std::move(tableIDToColumn));
        }
        return make_unique<ScanMultiTablePropertyColumn>(inputNodeIDVectorPos,
            std::move(outVectorsPos), std::move(outDataTypes), std::move(tableIDToColumns),
            std::move(prevOperator), getOperatorID(), scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = node->getTableID();
        vector<Column*> columns;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            columns.push_back(
                nodeStore.getNodePropertyColumn(tableID, property->getPropertyID(tableID)));
        }
        return make_unique<ScanSingleTablePropertyColumn>(inputNodeIDVectorPos,
            std::move(outVectorsPos), std::move(outDataTypes), std::move(columns),
            std::move(prevOperator), getOperatorID(), scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
