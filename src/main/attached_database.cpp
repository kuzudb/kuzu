#include "main/attached_database.h"

#include "common/types/types.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace main {

void AttachedDatabase::invalidateCache() {
    if (dbType != common::ATTACHED_KUZU_DB_TYPE) {
        auto catalogExtension = catalog->ptrCast<extension::CatalogExtension>();
        catalogExtension->invalidateCache();
    }
}

void AttachedKuzuDatabase::initCatalog(const std::string& path, ClientContext* context) {
    auto vfs = context->getVFSUnsafe();
    auto catalogFilePath =
        storage::StorageUtils::getCatalogFilePath(vfs, path, common::FileVersionType::ORIGINAL);
    if (!vfs->fileOrPathExists(catalogFilePath, context)) {
        throw common::RuntimeException(common::stringFormat(
            "Cannot attach a remote kuzu database due to invalid path: {}.", path));
    }
    catalog = std::make_unique<catalog::Catalog>();
    catalog->readFromFile(path, vfs, common::FileVersionType::ORIGINAL, context);
    catalog->registerBuiltInFunctions();
}

static void validateEmptyWAL(const std::string& path, ClientContext* context) {
    auto walFile = context->getVFSUnsafe()->openFile(
        path + "/" + common::StorageConstants::WAL_FILE_SUFFIX, O_RDONLY, context);
    if (walFile->getFileSize() > 0) {
        throw common::RuntimeException(
            common::stringFormat("Cannot attach a remote kuzu database with non-empty wal file."));
    }
}

AttachedKuzuDatabase::AttachedKuzuDatabase(std::string dbPath, std::string dbName,
    std::string dbType, ClientContext* clientContext)
    : AttachedDatabase{std::move(dbName), std::move(dbType), nullptr /* catalog */} {
    auto vfs = clientContext->getVFSUnsafe();
    auto path = vfs->expandPath(clientContext, dbPath);
    initCatalog(path, clientContext);
    validateEmptyWAL(path, clientContext);
    storageManager = std::make_unique<storage::StorageManager>(path, true /* isReadOnly */,
        *catalog, *clientContext->getMemoryManager(),
        clientContext->getDBConfig()->enableCompression, vfs, clientContext);
    transactionManager =
        std::make_unique<transaction::TransactionManager>(storageManager->getWAL());
}

} // namespace main
} // namespace kuzu
