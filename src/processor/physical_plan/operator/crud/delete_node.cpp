#include "src/processor/include/physical_plan/operator/crud/delete_node.h"

namespace graphflow {
namespace processor {

void DeleteNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    context.transaction->localStorage.mapNodeIDs(nodeLabel);
    context.transaction->localStorage.deleteNodeIDs(nodeLabel);
    context.transaction->localStorage.computeDeleteNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
}

} // namespace processor
} // namespace graphflow
