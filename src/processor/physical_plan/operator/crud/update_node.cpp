#include "src/processor/include/physical_plan/operator/crud/update_node.h"

namespace graphflow {
namespace processor {

void UpdateNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    context.transaction->localStorage.mapNodeIDs(nodeLabel);
    context.transaction->localStorage.computeUpdateNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
}

} // namespace processor
} // namespace graphflow
