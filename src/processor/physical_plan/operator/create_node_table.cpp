#include "src/processor/include/physical_plan/operator/create_node_table.h"

namespace graphflow {
namespace processor {

bool CreateNodeTable::getNextTuples() {
    auto newTableID = catalog->addNodeTableSchema(tableName, primaryKeyIdx, propertyNameDataTypes);
    nodesMetadata->addNodeMetadata(catalog->getWriteVersion()->getNodeTableSchema(newTableID));
    return false;
}

} // namespace processor
} // namespace graphflow
