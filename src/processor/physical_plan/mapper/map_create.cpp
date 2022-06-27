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
    for (auto i = 0u; i < logicalCreate.getNumCreateItems(); ++i) {
        auto [expression, setItems] = logicalCreate.getCreateItem(i);
        auto& node = (NodeExpression&)*expression;
        nodeTables.push_back(nodesStore.getNode(node.getLabel()));
    }
    return make_unique<CreateNode>(move(nodeTables), move(prevOperator), getOperatorID(),
        logicalCreate.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
