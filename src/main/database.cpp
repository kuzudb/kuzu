#include "main/database.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "common/exception/exception.h"
#include "common/file_utils.h"
#include "common/logging_level_utils.h"
#include "common/utils.h"
#include "function/scalar_function.h"
#include "processor/processor.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer.h"
#include "transaction/transaction_action.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

SystemConfig::SystemConfig(
    uint64_t bufferPoolSize_, uint64_t maxNumThreads, bool enableCompression, bool readOnly)
    : maxNumThreads{maxNumThreads}, enableCompression{enableCompression}, readOnly(readOnly) {
    if (bufferPoolSize_ == -1u || bufferPoolSize_ == 0) {
#if defined(_WIN32)
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        auto systemMemSize = (std::uint64_t)status.ullTotalPhys;
#else
        auto systemMemSize =
            (std::uint64_t)sysconf(_SC_PHYS_PAGES) * (std::uint64_t)sysconf(_SC_PAGESIZE);
#endif
        bufferPoolSize_ = (uint64_t)(BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM *
                                     (double_t)std::min(systemMemSize, (std::uint64_t)UINTPTR_MAX));
    }
    bufferPoolSize = bufferPoolSize_;
    if (maxNumThreads == 0) {
        this->maxNumThreads = std::thread::hardware_concurrency();
    }
}

static void getLockFileFlagsAndType(bool readOnly, bool createNew, int& flags, FileLockType& lock) {
    flags = readOnly ? O_RDONLY : O_RDWR;
    if (createNew && !readOnly) {
        flags |= O_CREAT;
    }
    lock = readOnly ? FileLockType::READ_LOCK : FileLockType::WRITE_LOCK;
}

Database::Database(std::string databasePath) : Database{std::move(databasePath), SystemConfig()} {}

Database::Database(std::string databasePath, SystemConfig systemConfig)
    : databasePath{std::move(databasePath)}, systemConfig{systemConfig} {
    initLoggers();
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::DATABASE);
    bufferManager = std::make_unique<BufferManager>(this->systemConfig.bufferPoolSize);
    memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
    queryProcessor = std::make_unique<processor::QueryProcessor>(this->systemConfig.maxNumThreads);
    initDBDirAndCoreFilesIfNecessary();
    wal = std::make_unique<WAL>(this->databasePath, systemConfig.readOnly, *bufferManager);
    recoverIfNecessary();
    catalog = std::make_unique<catalog::Catalog>(wal.get());
    storageManager = std::make_unique<storage::StorageManager>(
        systemConfig.readOnly, *catalog, *memoryManager, wal.get(), systemConfig.enableCompression);
    transactionManager =
        std::make_unique<transaction::TransactionManager>(*wal, memoryManager.get());
}

Database::~Database() {
    dropLoggers();
    bufferManager->clearEvictionQueue();
}

void Database::setLoggingLevel(std::string loggingLevel) {
    spdlog::set_level(LoggingLevelUtils::convertStrToLevelEnum(std::move(loggingLevel)));
}

void Database::addBuiltInFunction(std::string name, function::function_set functionSet) {
    catalog->addFunction(std::move(name), std::move(functionSet));
}

void Database::openLockFile() {
    int flags;
    FileLockType lock;
    auto lockFilePath = StorageUtils::getLockFilePath(databasePath);
    if (!FileUtils::fileOrPathExists(lockFilePath)) {
        getLockFileFlagsAndType(systemConfig.readOnly, true, flags, lock);
    } else {
        getLockFileFlagsAndType(systemConfig.readOnly, false, flags, lock);
    }
    lockFile = FileUtils::openFile(lockFilePath, flags, lock);
}

void Database::initDBDirAndCoreFilesIfNecessary() {
    if (!FileUtils::fileOrPathExists(databasePath)) {
        if (systemConfig.readOnly) {
            throw Exception("Cannot create an empty database under READ ONLY mode.");
        }
        FileUtils::createDir(databasePath);
    }
    openLockFile();
    if (!FileUtils::fileOrPathExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(
            databasePath, FileVersionType::ORIGINAL))) {
        NodesStoreStatsAndDeletedIDs::saveInitialNodesStatisticsAndDeletedIDsToFile(databasePath);
    }
    if (!FileUtils::fileOrPathExists(
            StorageUtils::getRelsStatisticsFilePath(databasePath, FileVersionType::ORIGINAL))) {
        RelsStoreStats::saveInitialRelsStatisticsToFile(databasePath);
    }
    if (!FileUtils::fileOrPathExists(
            StorageUtils::getCatalogFilePath(databasePath, FileVersionType::ORIGINAL))) {
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
    LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::WAL);
}

void Database::commit(Transaction* transaction, bool skipCheckpointForTestingRecovery) {
    if (transaction->isReadOnly()) {
        transactionManager->commit(transaction);
        return;
    }
    KU_ASSERT(transaction->isWriteTransaction());
    catalog->prepareCommitOrRollback(TransactionAction::COMMIT);
    storageManager->prepareCommit(transaction);
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
    transactionManager->commitButKeepActiveWriteTransaction(transaction);
    wal->flushAllPages();
    if (skipCheckpointForTestingRecovery) {
        transactionManager->allowReceivingNewTransactions();
        return;
    }
    checkpointAndClearWAL(WALReplayMode::COMMIT_CHECKPOINT);
    transactionManager->manuallyClearActiveWriteTransaction(transaction);
    transactionManager->allowReceivingNewTransactions();
}

void Database::rollback(
    transaction::Transaction* transaction, bool skipCheckpointForTestingRecovery) {
    if (transaction->isReadOnly()) {
        transactionManager->rollback(transaction);
        return;
    }
    KU_ASSERT(transaction->isWriteTransaction());
    catalog->prepareCommitOrRollback(TransactionAction::ROLLBACK);
    storageManager->prepareRollback(transaction);
    if (skipCheckpointForTestingRecovery) {
        wal->flushAllPages();
        return;
    }
    rollbackAndClearWAL();
    transactionManager->manuallyClearActiveWriteTransaction(transaction);
}

void Database::checkpointAndClearWAL(WALReplayMode replayMode) {
    KU_ASSERT(replayMode == WALReplayMode::COMMIT_CHECKPOINT ||
              replayMode == WALReplayMode::RECOVERY_CHECKPOINT);
    auto walReplayer = std::make_unique<WALReplayer>(
        wal.get(), storageManager.get(), bufferManager.get(), catalog.get(), replayMode);
    walReplayer->replay();
    wal->clearWAL();
}

void Database::rollbackAndClearWAL() {
    auto walReplayer = std::make_unique<WALReplayer>(wal.get(), storageManager.get(),
        bufferManager.get(), catalog.get(), WALReplayMode::ROLLBACK);
    walReplayer->replay();
    wal->clearWAL();
}

void Database::recoverIfNecessary() {
    if (!wal->isEmptyWAL()) {
        logger->info("Starting up StorageManager and found a non-empty WAL with a committed "
                     "transaction. Replaying to checkpointInMemory.");
        checkpointAndClearWAL(WALReplayMode::RECOVERY_CHECKPOINT);
    }
}

} // namespace main
} // namespace kuzu
