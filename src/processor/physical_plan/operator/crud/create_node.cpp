#include "src/processor/include/physical_plan/operator/crud/create_node.h"

namespace graphflow {
namespace processor {

void CreateNode::getNextTuples() {
    CRUDOperator::getNextTuples();
    transactionPtr->localStorage.computeCreateNode(
        move(propertyKeyVectorPos), nodeLabel, nodePropertyColumns, numNodes);
};

} // namespace processor
} // namespace graphflow
