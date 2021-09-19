#include "src/processor/include/physical_plan/operator/crud/update_node.h"

namespace graphflow {
namespace processor {

bool UpdateNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    context.transaction->localStorage.mapNodeIDs(nodeLabel);
    context.transaction->localStorage.computeUpdateNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
    return false;
}

} // namespace processor
} // namespace graphflow
