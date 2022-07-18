#include "src/processor/include/physical_plan/operator/create_node_table.h"

namespace graphflow {
namespace processor {

bool CreateNodeTable::getNextTuples() {
    auto newNodeLabel = catalog->createNodeLabel(labelName, primaryKey, propertyNameDataTypes);
    catalog->addNodeLabel(move(newNodeLabel));
    return false;
}

} // namespace processor
} // namespace graphflow
