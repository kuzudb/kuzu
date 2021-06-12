#include "src/processor/include/physical_plan/operator/crud/create_node.h"

namespace graphflow {
namespace processor {

void CreateNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    context.transaction->localStorage.assignNodeIDs(nodeLabel);
    context.transaction->localStorage.computeCreateNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
}

} // namespace processor
} // namespace graphflow
