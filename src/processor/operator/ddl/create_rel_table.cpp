#include "processor/operator/ddl/create_rel_table.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal(ExecutionContext* context) {
    auto newTableID = catalog->addRelTableSchema(info);
    auto newRelTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
        catalog->getTableCatalogEntry(context->clientContext->getTx(), newTableID));
    storageManager->getRelsStatistics()->addTableStatistic(newRelTableEntry);
    storageManager->createTable(newTableID, catalog, context->clientContext->getTx());
    storageManager->getWAL()->logCreateRelTableRecord(newTableID);
}

std::string CreateRelTable::getOutputMsg() {
    return stringFormat("Rel table: {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu
