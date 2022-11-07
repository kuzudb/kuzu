#include "include/database.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/include/wal_replayer.h"

namespace graphflow {
namespace main {

Database::Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig)
    : databaseConfig{databaseConfig},
      systemConfig{systemConfig}, logger{LoggerUtils::getOrCreateSpdLogger("database")} {
    initDBDirAndCoreFilesIfNecessary();
    bufferManager = make_unique<BufferManager>(
        systemConfig.defaultPageBufferPoolSize, systemConfig.largePageBufferPoolSize);
    memoryManager = make_unique<MemoryManager>(bufferManager.get());
    wal = make_unique<WAL>(databaseConfig.databasePath, *bufferManager);
    recoverIfNecessary();
    queryProcessor = make_unique<processor::QueryProcessor>(systemConfig.maxNumThreads);
    catalog = make_unique<catalog::Catalog>(wal.get());
    storageManager = make_unique<storage::StorageManager>(
        *catalog, *bufferManager, *memoryManager, databaseConfig.inMemoryMode, wal.get());
    transactionManager = make_unique<transaction::TransactionManager>(*wal);
}

void Database::initDBDirAndCoreFilesIfNecessary() {
    if (!FileUtils::fileOrPathExists(databaseConfig.databasePath)) {
        FileUtils::createDir(databaseConfig.databasePath);
    }
    if (!FileUtils::fileOrPathExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(
            databaseConfig.databasePath, DBFileType::ORIGINAL))) {
        NodesStatisticsAndDeletedIDs::saveInitialNodesStatisticsAndDeletedIDsToFile(
            databaseConfig.databasePath);
    }
    if (!FileUtils::fileOrPathExists(StorageUtils::getRelsStatisticsFilePath(
            databaseConfig.databasePath, DBFileType::ORIGINAL))) {
        RelsStatistics::saveInitialRelsStatisticsToFile(databaseConfig.databasePath);
    }
    if (!FileUtils::fileOrPathExists(
            StorageUtils::getCatalogFilePath(databaseConfig.databasePath, DBFileType::ORIGINAL))) {
        Catalog::saveInitialCatalogToFile(databaseConfig.databasePath);
    }
}

void Database::resizeBufferManager(uint64_t newSize) {
    systemConfig.defaultPageBufferPoolSize = newSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO;
    systemConfig.largePageBufferPoolSize = newSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO;
    bufferManager->resize(
        systemConfig.defaultPageBufferPoolSize, systemConfig.largePageBufferPoolSize);
}

void Database::recoverIfNecessary() {
    if (!wal->isEmptyWAL()) {
        if (wal->isLastLoggedRecordCommit()) {
            logger->info("Starting up StorageManager and found a non-empty WAL with a committed "
                         "transaction. Replaying to checkpoint.");
            checkpointOrRollbackAndClearWAL(true /* is recovering */, true /* checkpoint */);
        } else {
            logger->info("Starting up StorageManager and found a non-empty WAL but last record is "
                         "not commit. Clearing the WAL.");
            wal->clearWAL();
        }
    }
}

void Database::checkpointOrRollbackAndClearWAL(bool isRecovering, bool isCheckpoint) {
    logger->info(
        "Starting " +
        (isCheckpoint ? string("checkpointing") : string("rolling back the wal contents")) +
        " in the storage manager during " +
        (isRecovering ? "recovery." : "normal db execution (i.e., not recovering)."));
    WALReplayer walReplayer = isRecovering ?
                                  WALReplayer(wal.get()) :
                                  WALReplayer(wal.get(), storageManager.get(), bufferManager.get(),
                                      memoryManager.get(), catalog.get(), isCheckpoint);
    walReplayer.replay();
    logger->info(
        "Finished " +
        (isCheckpoint ? string("checkpointing") : string("rolling back the wal contents")) +
        " in the storage manager.");
    wal->clearWAL();
}

} // namespace main
} // namespace graphflow
