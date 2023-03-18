#include "main/database.h"

#include <utility>

#include "common/logging_level_utils.h"
#include "processor/processor.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

SystemConfig::SystemConfig() : SystemConfig(-1u) {}

SystemConfig::SystemConfig(uint64_t bufferPoolSize_) {
    if (bufferPoolSize_ == -1u) {
        auto systemMemSize =
            (std::uint64_t)sysconf(_SC_PHYS_PAGES) * (std::uint64_t)sysconf(_SC_PAGESIZE);
        bufferPoolSize_ = (uint64_t)(BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM *
                                     (double_t)std::min(systemMemSize, (std::uint64_t)UINTPTR_MAX));
    }
    bufferPoolSize = bufferPoolSize_;
    maxNumThreads = std::thread::hardware_concurrency();
}

Database::Database(std::string databasePath) : Database{std::move(databasePath), SystemConfig()} {}

Database::Database(std::string databasePath, SystemConfig systemConfig)
    : databasePath{std::move(databasePath)}, systemConfig{systemConfig} {
    initLoggers();
    initDBDirAndCoreFilesIfNecessary();
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::DATABASE);
    bufferManager = std::make_unique<BufferManager>(this->systemConfig.bufferPoolSize);
    memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
    wal = std::make_unique<WAL>(this->databasePath, *bufferManager);
    recoverIfNecessary();
    queryProcessor = std::make_unique<processor::QueryProcessor>(this->systemConfig.maxNumThreads);
    catalog = std::make_unique<catalog::Catalog>(wal.get());
    storageManager = std::make_unique<storage::StorageManager>(*catalog, *memoryManager, wal.get());
    transactionManager = std::make_unique<transaction::TransactionManager>(*wal);
}

Database::Database(const char* databasePath)
    : Database{std::string{databasePath}, SystemConfig()} {}

Database::Database(const char* databasePath, SystemConfig systemConfig)
    : Database{std::string{databasePath}, systemConfig} {}

Database::~Database() {
    dropLoggers();
}

void Database::initDBDirAndCoreFilesIfNecessary() const {
    if (!FileUtils::fileOrPathExists(databasePath)) {
        FileUtils::createDir(databasePath);
    }
    if (!FileUtils::fileOrPathExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(
            databasePath, DBFileType::ORIGINAL))) {
        NodesStatisticsAndDeletedIDs::saveInitialNodesStatisticsAndDeletedIDsToFile(databasePath);
    }
    if (!FileUtils::fileOrPathExists(
            StorageUtils::getRelsStatisticsFilePath(databasePath, DBFileType::ORIGINAL))) {
        RelsStatistics::saveInitialRelsStatisticsToFile(databasePath);
    }
    if (!FileUtils::fileOrPathExists(
            StorageUtils::getCatalogFilePath(databasePath, DBFileType::ORIGINAL))) {
        Catalog::saveInitialCatalogToFile(databasePath);
    }
}

void Database::initLoggers() {
    // To avoid multi-threading issue in creating logger, we create all loggers together with
    // database instance. All system components should get logger instead of creating.
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::DATABASE);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::CSV_READER);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::LOADER);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::PROCESSOR);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::CATALOG);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::TRANSACTION_MANAGER);
    LoggerUtils::createLogger(LoggerConstants::LoggerEnum::WAL);
    spdlog::set_level(spdlog::level::err);
}

void Database::dropLoggers() {
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::DATABASE);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::CSV_READER);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::LOADER);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::PROCESSOR);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::CATALOG);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::STORAGE);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::TRANSACTION_MANAGER);
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::WAL);
}

void Database::setLoggingLevel(std::string loggingLevel) {
    spdlog::set_level(LoggingLevelUtils::convertStrToLevelEnum(std::move(loggingLevel)));
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
                    .writeTablesStatisticsFileForWALRecord(databasePath);
            } else {
                storageManager->getRelsStore()
                    .getRelsStatistics()
                    .writeTablesStatisticsFileForWALRecord(databasePath);
            }
        }
    }
    if (catalog->hasUpdates()) {
        wal->logCatalogRecord();
        // If we are committing, we also need to write the WAL file for catalog.
        if (isCommit) {
            catalog->writeCatalogForWALRecord(databasePath);
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

void Database::checkpointAndClearWAL() {
    checkpointOrRollbackAndClearWAL(false /* is not recovering */, true /* isCheckpoint */);
}

void Database::rollbackAndClearWAL() {
    checkpointOrRollbackAndClearWAL(
        false /* is not recovering */, false /* rolling back updates */);
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
    WALReplayer walReplayer = isRecovering ? WALReplayer(wal.get()) :
                                             WALReplayer(wal.get(), storageManager.get(),
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
