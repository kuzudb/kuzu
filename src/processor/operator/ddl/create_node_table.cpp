#include "processor/operator/ddl/create_node_table.h"

#include "catalog/node_table_schema.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal() {
    auto newTableID = catalog->addNodeTableSchema(*info);
    auto newNodeTableSchema =
        reinterpret_cast<NodeTableSchema*>(catalog->getWriteVersion()->getTableSchema(newTableID));
    storageManager->getNodesStatisticsAndDeletedIDs()->addNodeStatisticsAndDeletedIDs(
        newNodeTableSchema);
    storageManager->getWAL()->logCreateNodeTableRecord(newTableID);
}

std::string CreateNodeTable::getOutputMsg() {
    return stringFormat("Node table: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu
