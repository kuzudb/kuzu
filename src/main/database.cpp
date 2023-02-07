#include "main/database.h"

#include "common/configs.h"
#include "common/logging_level_utils.h"
#include "spdlog/spdlog.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

SystemConfig::SystemConfig(uint64_t bufferPoolSize) {
    if (bufferPoolSize == -1u) {
        auto systemMemSize =
            (std::uint64_t)sysconf(_SC_PHYS_PAGES) * (std::uint64_t)sysconf(_SC_PAGESIZE);
        bufferPoolSize = (uint64_t)(StorageConfig::DEFAULT_BUFFER_POOL_RATIO *
                                    (double_t)std::min(systemMemSize, (std::uint64_t)UINTPTR_MAX));
    }
    defaultPageBufferPoolSize =
        (uint64_t)((double_t)bufferPoolSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO);
    largePageBufferPoolSize =
        (uint64_t)((double_t)bufferPoolSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO);
}

Database::Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig)
    : databaseConfig{databaseConfig},
      systemConfig{systemConfig}, logger{LoggerUtils::getOrCreateLogger("database")} {
    initLoggers();
    initDBDirAndCoreFilesIfNecessary();
    bufferManager = std::make_unique<BufferManager>(
        systemConfig.defaultPageBufferPoolSize, systemConfig.largePageBufferPoolSize);
    memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
    wal = std::make_unique<WAL>(databaseConfig.databasePath, *bufferManager);
    recoverIfNecessary();
    queryProcessor = std::make_unique<processor::QueryProcessor>(systemConfig.maxNumThreads);
    catalog = std::make_unique<catalog::Catalog>(wal.get());
    storageManager = std::make_unique<storage::StorageManager>(
        *catalog, *bufferManager, *memoryManager, wal.get());
    transactionManager = std::make_unique<transaction::TransactionManager>(*wal);
}

void Database::initDBDirAndCoreFilesIfNecessary() const {
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

void Database::initLoggers() {
    // To avoid multi-threading issue in creating logger, we create all loggers together with
    // database instance. All system components should get logger instead of creating.
    LoggerUtils::getOrCreateLogger("csv_reader");
    LoggerUtils::getOrCreateLogger("loader");
    LoggerUtils::getOrCreateLogger("processor");
    LoggerUtils::getOrCreateLogger("buffer_manager");
    LoggerUtils::getOrCreateLogger("catalog");
    LoggerUtils::getOrCreateLogger("storage");
    LoggerUtils::getOrCreateLogger("transaction_manager");
    LoggerUtils::getOrCreateLogger("wal");
    spdlog::set_level(spdlog::level::err);
}

void Database::setLoggingLevel(spdlog::level::level_enum loggingLevel) {
    if (loggingLevel != spdlog::level::level_enum::debug &&
        loggingLevel != spdlog::level::level_enum::info &&
        loggingLevel != spdlog::level::level_enum::err) {
        printf("Unsupported logging level: %s.",
            LoggingLevelUtils::convertLevelEnumToStr(loggingLevel).c_str());
        return;
    }
    spdlog::set_level(loggingLevel);
}

void Database::resizeBufferManager(uint64_t newSize) {
    systemConfig.defaultPageBufferPoolSize = newSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO;
    systemConfig.largePageBufferPoolSize = newSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO;
    bufferManager->resize(
        systemConfig.defaultPageBufferPoolSize, systemConfig.largePageBufferPoolSize);
}

void Database::commitAndCheckpointOrRollback(
    Transaction* writeTransaction, bool isCommit, bool skipCheckpointForTestingRecovery) {
    // Irrespective of whether we are checkpointing or rolling back we add a
    // nodesStatisticsAndDeletedIDs/relStatistics record if there has been updates to
    // nodesStatisticsAndDeletedIDs/relStatistics. This is because we need to commit or rollback
    // the in-memory state of NodesStatisticsAndDeletedIDs/relStatistics, which is done during
    // wal replaying and committing/rolling back each record, so a TABLE_STATISTICS_RECORD needs
    // to appear in the log.
    bool nodeTableHasUpdates =
        storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs().hasUpdates();
    bool relTableHasUpdates = storageManager->getRelsStore().getRelsStatistics().hasUpdates();
    if (nodeTableHasUpdates || relTableHasUpdates) {
        wal->logTableStatisticsRecord(nodeTableHasUpdates /* isNodeTable */);
        // If we are committing, we also need to write the WAL file for
        // NodesStatisticsAndDeletedIDs/relStatistics.
        if (isCommit) {
            if (nodeTableHasUpdates) {
                storageManager->getNodesStore()
                    .getNodesStatisticsAndDeletedIDs()
                    .writeTablesStatisticsFileForWALRecord(databaseConfig.databasePath);
            } else {
                storageManager->getRelsStore()
                    .getRelsStatistics()
                    .writeTablesStatisticsFileForWALRecord(databaseConfig.databasePath);
            }
        }
    }
    if (catalog->hasUpdates()) {
        wal->logCatalogRecord();
        // If we are committing, we also need to write the WAL file for catalog.
        if (isCommit) {
            catalog->writeCatalogForWALRecord(databaseConfig.databasePath);
        }
    }
    storageManager->prepareCommitOrRollbackIfNecessary(isCommit);

    if (isCommit) {
        // Note: It is enough to stop and wait transactions to leave the system instead of
        // for example checking on the query processor's task scheduler. This is because the
        // first and last steps that a connection performs when executing a query is to
        // start and commit/rollback transaction. The query processor also ensures that it
        // will only return results or error after all threads working on the tasks of a
        // query stop working on the tasks of the query and these tasks are removed from the
        // query.
        transactionManager->stopNewTransactionsAndWaitUntilAllReadTransactionsLeave();
        // Note: committing and stopping new transactions can be done in any order. This
        // order allows us to throw exceptions if we have to wait a lot to stop.
        transactionManager->commitButKeepActiveWriteTransaction(writeTransaction);
        wal->flushAllPages();
        if (skipCheckpointForTestingRecovery) {
            transactionManager->allowReceivingNewTransactions();
            return;
        }
        checkpointAndClearWAL();
    } else {
        if (skipCheckpointForTestingRecovery) {
            wal->flushAllPages();
            return;
        }
        rollbackAndClearWAL();
    }
    transactionManager->manuallyClearActiveWriteTransaction(writeTransaction);
    if (isCommit) {
        transactionManager->allowReceivingNewTransactions();
    }
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
    logger->info("Starting " +
                 (isCheckpoint ? std::string("checkpointing") :
                                 std::string("rolling back the wal contents")) +
                 " in the storage manager during " +
                 (isRecovering ? "recovery." : "normal db execution (i.e., not recovering)."));
    WALReplayer walReplayer = isRecovering ?
                                  WALReplayer(wal.get()) :
                                  WALReplayer(wal.get(), storageManager.get(), bufferManager.get(),
                                      memoryManager.get(), catalog.get(), isCheckpoint);
    walReplayer.replay();
    logger->info("Finished " +
                 (isCheckpoint ? std::string("checkpointing") :
                                 std::string("rolling back the wal contents")) +
                 " in the storage manager.");
    wal->clearWAL();
}

} // namespace main
} // namespace kuzu
