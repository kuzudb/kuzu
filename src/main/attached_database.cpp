#include "main/attached_database.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/checkpointer.h"
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

static void validateEmptyWAL(const std::string& path, ClientContext* context) {
    auto vfs = context->getVFSUnsafe();
    auto walFilePath = storage::StorageUtils::getWALFilePath(path);
    if (vfs->fileOrPathExists(walFilePath, context)) {
        auto walFile = vfs->openFile(walFilePath,
            common::FileOpenFlags(common::FileFlags::READ_ONLY), context);
        if (walFile->getFileSize() > 0) {
            throw common::RuntimeException(common::stringFormat(
                "Cannot attach an external Kuzu database with non-empty wal file. Try manually "
                "checkpointing the external database (i.e., run \"CHECKPOINT;\")."));
        }
    }
}

AttachedKuzuDatabase::AttachedKuzuDatabase(std::string dbPath, std::string dbName,
    std::string dbType, ClientContext* clientContext)
    : AttachedDatabase{std::move(dbName), std::move(dbType), nullptr /* catalog */} {
    auto vfs = clientContext->getVFSUnsafe();
    if (DBConfig::isDBPathInMemory(dbPath)) {
        throw common::RuntimeException("Cannot attach an in-memory Kuzu database. Please give a "
                                       "path to an on-disk Kuzu database directory.");
    }
    auto path = vfs->expandPath(clientContext, dbPath);
    // Note: S3 directory path may end with a '/'.
    if (path.ends_with('/')) {
        path = path.substr(0, path.size() - 1);
    }
    if (!vfs->fileOrPathExists(path, clientContext)) {
        throw common::RuntimeException(common::stringFormat(
            "Cannot attach a remote Kuzu database due to invalid path: {}.", path));
    }
    catalog = std::make_unique<catalog::Catalog>();
    validateEmptyWAL(path, clientContext);
    storageManager = std::make_unique<storage::StorageManager>(path, true /* isReadOnly */,
        *storage::MemoryManager::Get(*clientContext),
        clientContext->getDBConfig()->enableCompression, vfs);
    transactionManager =
        std::make_unique<transaction::TransactionManager>(storageManager->getWAL());

    storageManager->initDataFileHandle(vfs, clientContext);
    if (storageManager->getDataFH()->getNumPages() > 0) {
        storage::Checkpointer::readCheckpoint(clientContext, catalog.get(), storageManager.get());
    }
}

} // namespace main
} // namespace kuzu
