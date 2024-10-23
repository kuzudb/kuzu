#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "common/timer.h"
#include "common/types/value/value.h"
#include "function/table/scan_replacement.h"
#include "main/client_config.h"
#include "parser/statement.h"
#include "prepared_statement.h"
#include "processor/warning_context.h"
#include "query_result.h"
#include "transaction/transaction_context.h"

namespace kuzu {

namespace binder {
class Binder;
class ExpressionBinder;
} // namespace binder

namespace common {
class RandomEngine;
class TaskScheduler;
class ProgressBar;
} // namespace common

namespace extension {
struct ExtensionOptions;
}

namespace main {
struct DBConfig;
class Database;
class DatabaseManager;
class AttachedKuzuDatabase;

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class KUZU_API ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class binder::ExpressionBinder;

public:
    explicit ClientContext(Database* database);
    ~ClientContext();

    // Client config
    const ClientConfig* getClientConfig() const { return &clientConfig; }
    ClientConfig* getClientConfigUnsafe() { return &clientConfig; }
    const DBConfig* getDBConfig() const { return &dbConfig; }
    DBConfig* getDBConfigUnsafe() { return &dbConfig; }
    common::Value getCurrentSetting(const std::string& optionName);
    bool isOptionSet(const std::string& optionName) const;
    // Timer and timeout
    void interrupt() { activeQuery.interrupted = true; }
    bool interrupted() const { return activeQuery.interrupted; }
    bool hasTimeout() const { return clientConfig.timeoutInMS != 0; }
    void setQueryTimeOut(uint64_t timeoutInMS);
    uint64_t getQueryTimeOut() const;
    void startTimer();
    uint64_t getTimeoutRemainingInMS() const;
    void resetActiveQuery() { activeQuery.reset(); }

    // Parallelism
    void setMaxNumThreadForExec(uint64_t numThreads);
    uint64_t getMaxNumThreadForExec() const;

    // Transaction.
    transaction::Transaction* getTx() const;
    transaction::TransactionContext* getTransactionContext() const;

    // Progress bar
    common::ProgressBar* getProgressBar() const;

    // Replace function.
    void addScanReplace(function::ScanReplacement scanReplacement);
    std::unique_ptr<function::ScanReplacementData> tryReplace(const std::string& objectName) const;
    // Extension
    void setExtensionOption(std::string name, common::Value value);
    extension::ExtensionOptions* getExtensionOptions() const;
    std::string getExtensionDir() const;

    // Environment.
    std::string getEnvVariable(const std::string& name);

    // Database component getters.
    std::string getDatabasePath() const;
    Database* getDatabase() const { return localDatabase; }
    common::TaskScheduler* getTaskScheduler() const;
    DatabaseManager* getDatabaseManager() const;
    storage::StorageManager* getStorageManager() const;
    storage::MemoryManager* getMemoryManager();
    storage::WAL* getWAL() const;
    catalog::Catalog* getCatalog() const;
    transaction::TransactionManager* getTransactionManagerUnsafe() const;
    common::VirtualFileSystem* getVFSUnsafe() const;
    common::RandomEngine* getRandomEngine();

    // Query.
    std::unique_ptr<PreparedStatement> prepare(std::string_view query);
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams,
        std::optional<uint64_t> queryID = std::nullopt);
    std::unique_ptr<QueryResult> query(std::string_view queryStatement,
        std::optional<uint64_t> queryID = std::nullopt);
    void runQuery(std::string query);

    // only use for test framework
    std::vector<std::shared_ptr<parser::Statement>> parseQuery(std::string_view query);

    void setDefaultDatabase(AttachedKuzuDatabase* defaultDatabase_);
    bool hasDefaultDatabase();

    void addScalarFunction(std::string name, function::function_set definitions);
    void removeScalarFunction(std::string name);

    processor::WarningContext& getWarningContextUnsafe();
    const processor::WarningContext& getWarningContext() const;

    void cleanUP();

private:
    std::unique_ptr<QueryResult> query(std::string_view query, std::string_view encodedJoin,
        bool enumerateAllPlans = true, std::optional<uint64_t> queryID = std::nullopt);

    std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);

    std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);

    // when we do prepare, we will start a transaction for the query
    // when we execute after prepare in a same context, we set requireNewTx to false and will not
    // commit the transaction in prepare when we only prepare a query statement, we set requireNewTx
    // to true and will commit the transaction in prepare
    std::unique_ptr<PreparedStatement> prepareNoLock(
        std::shared_ptr<parser::Statement> parsedStatement, bool enumerateAllPlans = false,
        std::string_view joinOrder = std::string_view(), bool requireNewTx = true,
        std::optional<std::unordered_map<std::string, std::shared_ptr<common::Value>>> inputParams =
            std::nullopt);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_unique<common::Value>((T)arg.second);
        params.insert({name, std::move(val)});
        return executeWithParams(preparedStatement, std::move(params), args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeNoLock(PreparedStatement* preparedStatement,
        uint32_t planIdx = 0u, std::optional<uint64_t> queryID = std::nullopt);

    bool canExecuteWriteQuery();

    void runFuncInTransaction(const std::function<void(void)>& fun);

    // Client side configurable settings.
    ClientConfig clientConfig;
    // Database configurable settings.
    DBConfig& dbConfig;
    // Current query.
    ActiveQuery activeQuery;
    // Transaction context.
    std::unique_ptr<transaction::TransactionContext> transactionContext;
    // Replace external object as pointer Value;
    std::vector<function::ScanReplacement> scanReplacements;
    // Extension configurable settings.
    std::unordered_map<std::string, common::Value> extensionOptionValues;
    // Random generator for UUID.
    std::unique_ptr<common::RandomEngine> randomEngine;
    // Local database.
    Database* localDatabase;
    // Remote database.
    AttachedKuzuDatabase* remoteDatabase;
    // Progress bar.
    std::unique_ptr<common::ProgressBar> progressBar;
    // Warning information
    processor::WarningContext warningContext;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
