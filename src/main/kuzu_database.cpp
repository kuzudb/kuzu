#include "main/kuzu_database.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_manager.h"
#include "storage/wal/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace main {

// static void validateEmptyWAL(const std::string& path, ClientContext* context) {
//     auto vfs = context->getVFSUnsafe();
//     auto walFilePath = storage::StorageUtils::getWALFilePath(path);
//     if (vfs->fileOrPathExists(walFilePath, context)) {
//         auto walFile = vfs->openFile(walFilePath,
//             common::FileOpenFlags(common::FileFlags::READ_ONLY), context);
//         if (walFile->getFileSize() > 0) {
//             throw common::RuntimeException(common::stringFormat(
//                 "Cannot attach an external Kuzu database with non-empty wal file. Try manually "
//                 "checkpointing the external database (i.e., run \"CHECKPOINT;\")."));
//         }
//     }
// }

KuzuDatabase::KuzuDatabase(const std::string& dbPath, std::string dbName, std::string dbType,
    ClientContext* context)
    : DatabaseInstance{std::move(dbName), std::move(dbType), nullptr /* catalog */} {
    auto vfs = context->getVFSUnsafe();
    if (DBConfig::isDBPathInMemory(dbPath)) {
        throw RuntimeException("Cannot attach an in-memory Kuzu database. Please give a "
                               "path to an on-disk Kuzu database directory.");
    }
    auto path = vfs->expandPath(context, dbPath);
    // Note: S3 directory path may end with a '/'.
    if (path.ends_with('/')) {
        path = path.substr(0, path.size() - 1);
    }
    if (!vfs->fileOrPathExists(path, context)) {
        throw RuntimeException(
            stringFormat("Cannot attach a remote Kuzu database due to invalid path: {}.", path));
    }
    catalog = std::make_unique<catalog::Catalog>();
    // validateEmptyWAL(path, clientContext);
    storageManager = std::make_unique<StorageManager>(path, true /* isReadOnly */,
        *context->getMemoryManager(), context->getDBConfig()->enableCompression, vfs, context);
}

void KuzuDatabase::recover(ClientContext& context) {
    try {
        const auto walReplayer = std::make_unique<WALReplayer>(*context);
        walReplayer->replay();
    } catch (std::exception& e) {
        throw Exception(stringFormat("Error during recovery: {}", e.what()));
    }
}

} // namespace main
} // namespace kuzu
