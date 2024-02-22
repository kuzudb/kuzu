#include "processor/operator/ddl/create_node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal(ExecutionContext* context) {
    auto newTableID = catalog->addNodeTableSchema(info);
    auto newNodeTableEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(context->clientContext->getTx(), newTableID));
    storageManager->getNodesStatisticsAndDeletedIDs()->addNodeStatisticsAndDeletedIDs(
        newNodeTableEntry);
    storageManager->createTable(newTableID, catalog, context->clientContext->getTx());
    storageManager->getWAL()->logCreateNodeTableRecord(newTableID);
}

std::string CreateNodeTable::getOutputMsg() {
    return stringFormat("Node table: {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu
