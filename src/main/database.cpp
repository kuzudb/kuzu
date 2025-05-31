#include "main/database.h"

#include "extension/extension_manager.h"
#include "main/client_context.h"
#include "main/database_manager.h"
#include "storage/buffer_manager/buffer_manager.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "common/exception/exception.h"
#include "common/file_system/virtual_file_system.h"
#include "main/db_config.h"
#include "processor/processor.h"
#include "storage/checkpointer.h"
#include "storage/storage_extension.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

SystemConfig::SystemConfig(uint64_t bufferPoolSize_, uint64_t maxNumThreads, bool enableCompression,
    bool readOnly, uint64_t maxDBSize, bool autoCheckpoint, uint64_t checkpointThreshold)
    : maxNumThreads{maxNumThreads}, enableCompression{enableCompression}, readOnly{readOnly},
      autoCheckpoint{autoCheckpoint}, checkpointThreshold{checkpointThreshold} {
    if (bufferPoolSize_ == -1u || bufferPoolSize_ == 0) {
#if defined(_WIN32)
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        auto systemMemSize = (std::uint64_t)status.ullTotalPhys;
#else
        auto systemMemSize = static_cast<std::uint64_t>(sysconf(_SC_PHYS_PAGES)) *
                             static_cast<std::uint64_t>(sysconf(_SC_PAGESIZE));
#endif
        bufferPoolSize_ = static_cast<uint64_t>(
            BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM *
            static_cast<double>(std::min(systemMemSize, static_cast<uint64_t>(UINTPTR_MAX))));
        // On 32-bit systems or systems with extremely large memory, the buffer pool size may
        // exceed the maximum size of a VMRegion. In this case, we set the buffer pool size to
        // 80% of the maximum size of a VMRegion.
        bufferPoolSize_ = static_cast<uint64_t>(std::min(static_cast<double>(bufferPoolSize_),
            BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE *
                BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM));
    }
    bufferPoolSize = bufferPoolSize_;
#ifndef __SINGLE_THREADED__
    if (maxNumThreads == 0) {
        this->maxNumThreads = std::thread::hardware_concurrency();
    }
#else
    // In single-threaded mode, even if the user specifies a number of threads,
    // it will be ignored and set to 0.
    this->maxNumThreads = 1;
#endif
    if (maxDBSize == -1u) {
        maxDBSize = BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE;
    }
    this->maxDBSize = maxDBSize;
}

static void getLockFileFlagsAndType(bool readOnly, bool createNew, int& flags, FileLockType& lock) {
    flags = readOnly ? FileFlags::READ_ONLY : FileFlags::WRITE;
    if (createNew && !readOnly) {
        flags |= FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS;
    }
    lock = readOnly ? FileLockType::READ_LOCK : FileLockType::WRITE_LOCK;
}

Database::Database(std::string_view databasePath, SystemConfig systemConfig)
    : dbConfig{systemConfig} {
    initMembers(databasePath);
}

Database::Database(std::string_view databasePath, SystemConfig systemConfig,
    construct_bm_func_t constructBMFunc)
    : dbConfig(systemConfig) {
    initMembers(databasePath, constructBMFunc);
}

std::unique_ptr<BufferManager> Database::initBufferManager(const Database& db) {
    return std::make_unique<BufferManager>(db.databasePath,
        db.vfs->joinPath(db.databasePath, StorageConstants::TEMP_SPILLING_FILE_NAME),
        db.dbConfig.bufferPoolSize, db.dbConfig.maxDBSize, db.vfs.get(), db.dbConfig.readOnly);
}

void Database::initMembers(std::string_view dbPath, construct_bm_func_t initBmFunc) {
    // To expand a path with home directory(~), we have to pass in a dummy clientContext which
    // handles the home directory expansion.
    const auto dbPathStr = std::string(dbPath);
    auto clientContext = ClientContext(this);
    databasePath = StorageUtils::expandPath(&clientContext, dbPathStr);

    vfs = std::make_unique<VirtualFileSystem>(databasePath);
    initAndLockDBDir();

    bufferManager = initBmFunc(*this);
    memoryManager = std::make_unique<MemoryManager>(bufferManager.get(), vfs.get());
    queryProcessor = std::make_unique<processor::QueryProcessor>(dbConfig.maxNumThreads);
    catalog = std::make_unique<Catalog>();
    storageManager = std::make_unique<StorageManager>(dbPathStr, dbConfig.readOnly, *memoryManager,
        dbConfig.enableCompression, vfs.get(), &clientContext);
    transactionManager = std::make_unique<TransactionManager>(storageManager->getWAL());
    databaseManager = std::make_unique<DatabaseManager>();
    extensionManager = std::make_unique<extension::ExtensionManager>();
    extensionManager->autoLoadLinkedExtensions(&clientContext);

    Checkpointer checkpointer(clientContext);
    checkpointer.readCheckpoint();
    StorageManager::recover(clientContext);
}

Database::~Database() {
    if (!dbConfig.readOnly && dbConfig.forceCheckpointOnClose) {
        try {
            ClientContext clientContext(this);
            transactionManager->checkpoint(clientContext);
        } catch (...) {} // NOLINT
    }
}

void Database::registerFileSystem(std::unique_ptr<FileSystem> fs) {
    vfs->registerFileSystem(std::move(fs));
}

void Database::registerStorageExtension(std::string name,
    std::unique_ptr<StorageExtension> storageExtension) {
    extensionManager->registerStorageExtension(std::move(name), std::move(storageExtension));
}

void Database::addExtensionOption(std::string name, LogicalTypeID type, Value defaultValue,
    bool isConfidential) {
    extensionManager->addExtensionOption(name, type, std::move(defaultValue), isConfidential);
}

std::vector<StorageExtension*> Database::getStorageExtensions() {
    return extensionManager->getStorageExtensions();
}

void Database::openLockFile() {
    int flags = 0;
    FileLockType lock{};
    auto lockFilePath = StorageUtils::getLockFilePath(vfs.get(), databasePath);
    if (!vfs->fileOrPathExists(lockFilePath)) {
        getLockFileFlagsAndType(dbConfig.readOnly, true, flags, lock);
    } else {
        getLockFileFlagsAndType(dbConfig.readOnly, false, flags, lock);
    }
    FileOpenFlags openFlags{flags};
    openFlags.lockType = lock;
    lockFile = vfs->openFile(lockFilePath, openFlags, nullptr /* clientContext */);
}

void Database::initAndLockDBDir() {
    if (DBConfig::isDBPathInMemory(databasePath)) {
        if (dbConfig.readOnly) {
            throw Exception("Cannot open an in-memory database under READ ONLY mode.");
        }
        return;
    }
    if (!vfs->fileOrPathExists(databasePath)) {
        if (dbConfig.readOnly) {
            throw Exception("Cannot create an empty database under READ ONLY mode.");
        }
        vfs->createDir(databasePath);
    }
    openLockFile();
}

uint64_t Database::getNextQueryID() {
    std::unique_lock lock(queryIDGenerator.queryIDLock);
    return queryIDGenerator.queryID++;
}

} // namespace main
} // namespace kuzu
