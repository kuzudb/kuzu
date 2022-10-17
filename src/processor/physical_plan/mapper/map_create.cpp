#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/update/create.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalCreate = (LogicalCreate&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodesStore = storageManager.getNodesStore();
    vector<NodeTable*> nodeTables;
    vector<DataPos> outVectorPositions;
    for (auto& node : logicalCreate.getNodes()) {
        outVectorPositions.push_back(mapperContext.getDataPos(node->getIDProperty()));
        nodeTables.push_back(nodesStore.getNodeTable(node->getTableID()));
    }
    return make_unique<CreateNode>(std::move(nodeTables), std::move(outVectorPositions),
        std::move(prevOperator), getOperatorID(), logicalCreate.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
