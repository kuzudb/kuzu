#include "src/processor/include/physical_plan/operator/crud/delete_node.h"

namespace graphflow {
namespace processor {

bool DeleteNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    context.transaction->localStorage.mapNodeIDs(nodeLabel);
    context.transaction->localStorage.deleteNodeIDs(nodeLabel);
    context.transaction->localStorage.computeDeleteNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
    return false;
}

} // namespace processor
} // namespace graphflow
