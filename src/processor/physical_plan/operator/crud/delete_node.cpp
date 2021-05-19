#include "src/processor/include/physical_plan/operator/crud/delete_node.h"

namespace graphflow {
namespace processor {

void DeleteNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    transactionPtr->localStorage.mapNodeIDs(nodeLabel);
    transactionPtr->localStorage.deleteNodeIDs(nodeLabel);
    transactionPtr->localStorage.computeDeleteNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
};

} // namespace processor
} // namespace graphflow
