#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

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
namespace parser {
class StandaloneCallRewriter;
} // namespace parser

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
class ExtensionManager;
} // namespace extension

namespace processor {
class ImportDB;
class TableFunctionCall;
} // namespace processor

namespace graph {
class GraphEntrySet;
}

namespace main {
struct DBConfig;
class Database;
class DatabaseManager;
class AttachedKuzuDatabase;
struct SpillToDiskSetting;
struct ExtensionOption;
class EmbeddedShell;

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so the profiler
 * is not maintained in the client context.
 */
class KUZU_API ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class binder::ExpressionBinder;
    friend class processor::ImportDB;
    friend class processor::TableFunctionCall;
    friend class parser::StandaloneCallRewriter;
    friend struct SpillToDiskSetting;
    friend class EmbeddedShell;
    friend class extension::ExtensionManager;

public:
    explicit ClientContext(Database* database);
    ~ClientContext();

    // Client config
    const ClientConfig* getClientConfig() const { return &clientConfig; }
    ClientConfig* getClientConfigUnsafe() { return &clientConfig; }
    const DBConfig* getDBConfig() const { return &dbConfig; }
    DBConfig* getDBConfigUnsafe() { return &dbConfig; }
    common::Value getCurrentSetting(const std::string& optionName) const;
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
    transaction::Transaction* getTransaction() const;
    transaction::TransactionContext* getTransactionContext() const;

    // Progress bar
    common::ProgressBar* getProgressBar() const;

    // Replace function.
    void addScanReplace(function::ScanReplacement scanReplacement);
    std::unique_ptr<function::ScanReplacementData> tryReplaceByName(
        const std::string& objectName) const;
    std::unique_ptr<function::ScanReplacementData> tryReplaceByHandle(
        function::scan_replace_handle_t handle) const;
    // Extension
    void setExtensionOption(std::string name, common::Value value);
    const ExtensionOption* getExtensionOption(std::string optionName) const;
    std::string getExtensionDir() const;

    // Database component getters.
    std::string getDatabasePath() const;
    Database* getDatabase() const { return localDatabase; }
    common::TaskScheduler* getTaskScheduler() const;
    DatabaseManager* getDatabaseManager() const;
    storage::StorageManager* getStorageManager() const;
    storage::MemoryManager* getMemoryManager() const;
    extension::ExtensionManager* getExtensionManager() const;
    storage::WAL* getWAL() const;
    catalog::Catalog* getCatalog() const;
    transaction::TransactionManager* getTransactionManagerUnsafe() const;
    common::VirtualFileSystem* getVFSUnsafe() const;
    common::RandomEngine* getRandomEngine() const;

    static std::string getEnvVariable(const std::string& name);

    void setDefaultDatabase(AttachedKuzuDatabase* defaultDatabase_);
    bool hasDefaultDatabase() const;
    void setUseInternalCatalogEntry(bool useInternalCatalogEntry) {
        this->useInternalCatalogEntry_ = useInternalCatalogEntry;
    }
    bool useInternalCatalogEntry() const {
        return clientConfig.enableInternalCatalog ? true : useInternalCatalogEntry_;
    }

    void addScalarFunction(std::string name, function::function_set definitions);
    void removeScalarFunction(const std::string& name);

    processor::WarningContext& getWarningContextUnsafe();
    const processor::WarningContext& getWarningContext() const;

    graph::GraphEntrySet& getGraphEntrySetUnsafe();

    const graph::GraphEntrySet& getGraphEntrySet() const;

    void cleanUp();

    // Query.
    std::unique_ptr<PreparedStatement> prepareWithParams(std::string_view query,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams = {});
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams,
        std::optional<uint64_t> queryID = std::nullopt);
    std::unique_ptr<QueryResult> query(std::string_view queryStatement,
        std::optional<uint64_t> queryID = std::nullopt);

private:
    struct TransactionHelper {
        enum class TransactionCommitAction : uint8_t {
            COMMIT_IF_NEW,
            COMMIT_IF_AUTO,
            COMMIT_NEW_OR_AUTO,
            NOT_COMMIT
        };
        static bool commitIfNew(TransactionCommitAction action) {
            return action == TransactionCommitAction::COMMIT_IF_NEW ||
                   action == TransactionCommitAction::COMMIT_NEW_OR_AUTO;
        }
        static bool commitIfAuto(TransactionCommitAction action) {
            return action == TransactionCommitAction::COMMIT_IF_AUTO ||
                   action == TransactionCommitAction::COMMIT_NEW_OR_AUTO;
        }
        static TransactionCommitAction getAction(bool commitIfNew, bool commitIfAuto);
        static void runFuncInTransaction(transaction::TransactionContext& context,
            const std::function<void()>& fun, bool readOnlyStatement, bool isTransactionStatement,
            TransactionCommitAction action);
    };

    static std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);
    static std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);
    static void bindParametersNoLock(const PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);
    void validateTransaction(const PreparedStatement& preparedStatement) const;

    std::vector<std::shared_ptr<parser::Statement>> parseQuery(std::string_view query);

    std::unique_ptr<PreparedStatement> prepareNoLock(
        std::shared_ptr<parser::Statement> parsedStatement, bool shouldCommitNewTransaction,
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

    std::unique_ptr<QueryResult> executeNoLock(PreparedStatement* preparedStatement,
        std::optional<uint64_t> queryID = std::nullopt);

    std::unique_ptr<QueryResult> queryNoLock(std::string_view query,
        std::optional<uint64_t> queryID = std::nullopt);

    bool canExecuteWriteQuery() const;

    std::unique_ptr<QueryResult> handleFailedExecution(std::optional<uint64_t> queryID,
        const std::exception& e) const;

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
    // Graph entries
    std::unique_ptr<graph::GraphEntrySet> graphEntrySet;
    std::mutex mtx;
    // Whether the query can access internal tables/sequences or not.
    bool useInternalCatalogEntry_ = false;
};

} // namespace main
} // namespace kuzu
