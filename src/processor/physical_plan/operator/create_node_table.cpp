#include "src/processor/include/physical_plan/operator/create_node_table.h"

namespace graphflow {
namespace processor {

bool CreateNodeTable::getNextTuples() {
    auto newNodeLabelID =
        catalog->createAndAddNodeLabel(labelName, primaryKey, propertyNameDataTypes);
    nodesMetadata->addNodeMetadata(catalog->getWriteVersion()->getNodeLabel(newNodeLabelID));
    return false;
}

} // namespace processor
} // namespace graphflow
