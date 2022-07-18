#include "src/processor/include/physical_plan/operator/create_node_table.h"

namespace graphflow {
namespace processor {

void CreateNodeTable::execute() {
    auto newNodeLabel = catalog->createNodeLabel(labelName, primaryKey, propertyNameDataTypes);
    catalog->addNodeLabel(move(newNodeLabel));
    storageManager->getNodesStore().addNode(
        *catalog, labelName, bufferManager, inMemoryMode, databasePath, &storageManager->getWAL());
}

} // namespace processor
} // namespace graphflow
