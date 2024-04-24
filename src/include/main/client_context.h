#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "client_config.h"
#include "common/task_system/progress_bar.h"
#include "common/timer.h"
#include "common/types/value/value.h"
#include "function/table/scan_replacement.h"
#include "parser/statement.h"
#include "prepared_statement.h"
#include "query_result.h"
#include "transaction/transaction_context.h"

namespace kuzu {

namespace binder {
class Binder;
class ExpressionBinder;
} // namespace binder

namespace common {
class RandomEngine;
}

namespace extension {
struct ExtensionOptions;
}

namespace main {
class Database;
class DatabaseManager;

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
class ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class binder::ExpressionBinder;

public:
    explicit ClientContext(Database* database);

    // Client config
    const ClientConfig* getClientConfig() const { return &config; }
    ClientConfig* getClientConfigUnsafe() { return &config; }
    KUZU_API common::Value getCurrentSetting(const std::string& optionName);
    // Timer and timeout
    void interrupt() { activeQuery.interrupted = true; }
    bool interrupted() const { return activeQuery.interrupted; }
    bool hasTimeout() const { return config.timeoutInMS != 0; }
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
    KUZU_API transaction::TransactionContext* getTransactionContext() const;

    // Progress bar
    common::ProgressBar* getProgressBar() const;

    // Replace function.
    void addScanReplace(function::ScanReplacement scanReplacement);
    std::unique_ptr<function::ScanReplacementData> tryReplace(const std::string& objectName) const;
    // Extension
    KUZU_API void setExtensionOption(std::string name, common::Value value);
    extension::ExtensionOptions* getExtensionOptions() const;
    std::string getExtensionDir() const;

    // Environment.
    KUZU_API std::string getEnvVariable(const std::string& name);

    // Database component getters.
    KUZU_API Database* getDatabase() const { return database; }
    KUZU_API DatabaseManager* getDatabaseManager() const;
    storage::StorageManager* getStorageManager() const;
    KUZU_API storage::MemoryManager* getMemoryManager();
    catalog::Catalog* getCatalog() const;
    common::VirtualFileSystem* getVFSUnsafe() const;
    common::RandomEngine* getRandomEngine();

    // Query.
    std::unique_ptr<PreparedStatement> prepare(std::string_view query);
    KUZU_API std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams);
    std::unique_ptr<QueryResult> query(std::string_view queryStatement);
    void runQuery(std::string query);

    // TODO(Jiamin): should remove after supporting ddl in manual tx
    std::unique_ptr<PreparedStatement> prepareTest(std::string_view query);
    // only use for test framework
    std::vector<std::shared_ptr<parser::Statement>> parseQuery(std::string_view query);

private:
    std::unique_ptr<QueryResult> query(std::string_view query, std::string_view encodedJoin,
        bool enumerateAllPlans = true);

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

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u, bool requiredNexTx = true);

    void addScalarFunction(std::string name, function::function_set definitions);

    bool startUDFAutoTrx(transaction::TransactionContext* trx);

    void commitUDFTrx(bool isAutoCommitTrx);

    // Client side configurable settings.
    ClientConfig config;
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
    // Attached database.
    Database* database;
    // Progress bar for queries
    std::unique_ptr<common::ProgressBar> progressBar;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
