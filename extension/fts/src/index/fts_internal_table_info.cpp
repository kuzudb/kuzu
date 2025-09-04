#include "index/fts_internal_table_info.h"

#include "storage/storage_manager.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

FTSInternalTableInfo::FTSInternalTableInfo(main::ClientContext* context, common::table_id_t tableID,
    const std::string& indexName, const std::string& stopWordsTableName) {
    auto docTableName = FTSUtils::getDocsTableName(tableID, indexName);
    auto termsTableName = FTSUtils::getTermsTableName(tableID, indexName);
    auto appearsInTableName = FTSUtils::getAppearsInTableName(tableID, indexName);
    auto storageManager = storage::StorageManager::Get(*context);
    auto catalog = catalog::Catalog::Get(*context);
    auto transaction = transaction::Transaction::Get(*context);
    table = storageManager->getTable(tableID)->ptrCast<storage::NodeTable>();
    stopWordsTable = storageManager->getTable(catalog->getTableCatalogEntry(transaction, stopWordsTableName)
                           ->getTableID())
            ->ptrCast<storage::NodeTable>();
    docTable = storageManager->getTable(catalog->getTableCatalogEntry(transaction, docTableName)
                                  ->getTableID())
                   ->ptrCast<storage::NodeTable>();
    termsTable = storageManager->getTable(catalog->getTableCatalogEntry(transaction, termsTableName)
                           ->getTableID())
            ->ptrCast<storage::NodeTable>();
    auto appearsInTableEntry =
        catalog->getTableCatalogEntry(transaction, appearsInTableName)
            ->constPtrCast<catalog::RelGroupCatalogEntry>()
            ->getRelEntryInfo(termsTable->getTableID(), docTable->getTableID());
    appearsInfoTable =
        storageManager->getTable(appearsInTableEntry->oid)->ptrCast<storage::RelTable>();
    dfColumnID =
        catalog->getTableCatalogEntry(transaction, termsTableName)->getColumnID("df");
}

} // namespace fts_extension
} // namespace kuzu
