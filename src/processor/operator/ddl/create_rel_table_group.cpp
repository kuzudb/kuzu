#include "processor/operator/ddl/create_rel_table_group.h"

#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRelTableGroup::executeDDLInternal(ExecutionContext* context) {
    auto newRelTableGroupID = catalog->addRelTableGroupSchema(info);
    auto tx = context->clientContext->getTx();
    auto newRelGroupEntry = ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(
        catalog->getTableCatalogEntry(tx, newRelTableGroupID));
    for (auto& relTableID : newRelGroupEntry->getRelTableIDs()) {
        auto newRelTableEntry = catalog->getTableCatalogEntry(tx, relTableID);
        storageManager->getRelsStatistics()->addTableStatistic(
            ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(newRelTableEntry));
        storageManager->createTable(relTableID, catalog, tx);
    }
    // TODO(Ziyi): remove this when we can log variable size record. See also wal_record.h
    for (auto relTableID : newRelGroupEntry->getRelTableIDs()) {
        storageManager->getWAL()->logCreateRelTableRecord(relTableID);
    }
}

std::string CreateRelTableGroup::getOutputMsg() {
    return stringFormat("Rel table group: {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu
