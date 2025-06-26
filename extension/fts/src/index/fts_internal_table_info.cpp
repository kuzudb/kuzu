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
    auto storageManager = context->getStorageManager();
    auto catalog = context->getCatalog();
    table = storageManager->getTable(tableID)->ptrCast<storage::NodeTable>();
    stopWordsTable =
        storageManager
            ->getTable(
                catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, stopWordsTableName)
                    ->getTableID())
            ->ptrCast<storage::NodeTable>();
    docTable =
        storageManager
            ->getTable(catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, docTableName)
                           ->getTableID())
            ->ptrCast<storage::NodeTable>();
    termsTable =
        storageManager
            ->getTable(
                catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, termsTableName)
                    ->getTableID())
            ->ptrCast<storage::NodeTable>();
    auto appearsInTableEntry =
        catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, appearsInTableName)
            ->constPtrCast<catalog::RelGroupCatalogEntry>()
            ->getRelEntryInfo(termsTable->getTableID(), docTable->getTableID());
    appearsInfoTable =
        storageManager->getTable(appearsInTableEntry->oid)->ptrCast<storage::RelTable>();
    dfColumnID = catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, termsTableName)
                     ->getColumnID("df");
}

} // namespace fts_extension
} // namespace kuzu
