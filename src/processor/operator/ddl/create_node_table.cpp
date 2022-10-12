#include "include/create_node_table.h"

namespace graphflow {
namespace processor {

bool CreateNodeTable::getNextTuples() {
    auto newTableID = catalog->addNodeTableSchema(tableName, primaryKeyIdx, propertyNameDataTypes);
    nodesStatisticsAndDeletedIDs->addNodeStatisticsAndDeletedIDs(
        catalog->getWriteVersion()->getNodeTableSchema(newTableID));
    return false;
}

} // namespace processor
} // namespace graphflow
