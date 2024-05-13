#include "main/attached_database.h"

#include "main/db_config.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace main {

void AttachedDatabase::invalidateCache() {
    if (dbType != common::ATTACHED_KUZU_DB_TYPE) {
        auto catalogExtension = catalog->ptrCast<extension::CatalogExtension>();
        catalogExtension->invalidateCache();
    }
}

AttachedKuzuDatabase::AttachedKuzuDatabase(std::string dbPath, std::string dbName,
    std::string dbType, ClientContext* clientContext)
    : AttachedDatabase{std::move(dbName), std::move(dbType), nullptr /* catalog */} {
    auto vfs = clientContext->getVFSUnsafe();
    auto path = vfs->expandPath(clientContext, dbPath);
    if (!vfs->fileOrPathExists(path)) {
        throw common::RuntimeException(common::stringFormat(
            "Cannot attach a remote kuzu database due to invalid path: {}.", dbPath));
    }
    auto walFile = vfs->openFile(path + "/" + common::StorageConstants::WAL_FILE_SUFFIX, O_RDONLY);
    if (walFile->getFileSize() > 0) {
        throw common::RuntimeException(
            common::stringFormat("Cannot attach a remote kuzu database with non-empty wal file."));
    }
    catalog = std::make_unique<catalog::Catalog>(path, vfs);
    storageManager = std::make_unique<storage::StorageManager>(path, true /* isReadOnly */,
        *catalog, *clientContext->getMemoryManager(),
        clientContext->getDBConfig()->enableCompression, vfs);
    transactionManager =
        std::make_unique<transaction::TransactionManager>(storageManager->getWAL());
}

} // namespace main
} // namespace kuzu
