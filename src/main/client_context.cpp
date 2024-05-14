#include "main/client_context.h"

#include "binder/binder.h"
#include "common/exception/connection.h"
#include "common/exception/runtime.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "main/attached_database.h"
#include "main/database.h"
#include "main/database_manager.h"
#include "main/db_config.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "parser/visitor/statement_read_write_analyzer.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/planner.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_context.h"

#if defined(_WIN32)
#include "common/windows_utils.h"
#endif

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

ActiveQuery::ActiveQuery() : interrupted{false} {}

void ActiveQuery::reset() {
    interrupted = false;
    timer = Timer();
}

ClientContext::ClientContext(Database* database)
    : dbConfig{database->dbConfig}, localDatabase{database} {
    progressBar = std::make_unique<common::ProgressBar>();
    transactionContext = std::make_unique<TransactionContext>(*this);
    randomEngine = std::make_unique<common::RandomEngine>();
    remoteDatabase = nullptr;
#if defined(_WIN32)
    clientConfig.homeDirectory = getEnvVariable("USERPROFILE");
#else
    clientConfig.homeDirectory = getEnvVariable("HOME");
#endif
    clientConfig.fileSearchPath = "";
    clientConfig.enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
    clientConfig.numThreads = database->dbConfig.maxNumThreads;
    clientConfig.timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
    clientConfig.varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
    clientConfig.enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
    clientConfig.showProgressAfter = ClientConfigDefault::SHOW_PROGRESS_AFTER;
    clientConfig.recursivePatternSemantic = ClientConfigDefault::RECURSIVE_PATTERN_SEMANTIC;
    clientConfig.recursivePatternCardinalityScaleFactor =
        ClientConfigDefault::RECURSIVE_PATTERN_FACTOR;
}

uint64_t ClientContext::getTimeoutRemainingInMS() const {
    KU_ASSERT(hasTimeout());
    auto elapsed = activeQuery.timer.getElapsedTimeInMS();
    return elapsed >= clientConfig.timeoutInMS ? 0 : clientConfig.timeoutInMS - elapsed;
}

void ClientContext::startTimer() {
    if (hasTimeout()) {
        activeQuery.timer.start();
    }
}

void ClientContext::setQueryTimeOut(uint64_t timeoutInMS) {
    lock_t lck{mtx};
    clientConfig.timeoutInMS = timeoutInMS;
}

uint64_t ClientContext::getQueryTimeOut() const {
    return clientConfig.timeoutInMS;
}

void ClientContext::setMaxNumThreadForExec(uint64_t numThreads) {
    lock_t lck{mtx};
    clientConfig.numThreads = numThreads;
}

uint64_t ClientContext::getMaxNumThreadForExec() const {
    return clientConfig.numThreads;
}

Value ClientContext::getCurrentSetting(const std::string& optionName) {
    auto lowerCaseOptionName = optionName;
    StringUtils::toLower(lowerCaseOptionName);
    // Firstly, try to find in built-in options.
    auto option = main::DBConfig::getOptionByName(lowerCaseOptionName);
    if (option != nullptr) {
        return option->getSetting(this);
    }
    // Secondly, try to find in current client session.
    if (extensionOptionValues.contains(lowerCaseOptionName)) {
        return extensionOptionValues.at(lowerCaseOptionName);
    }
    // Lastly, find the default value in db clientConfig.
    auto defaultOption = localDatabase->extensionOptions->getExtensionOption(lowerCaseOptionName);
    if (defaultOption != nullptr) {
        return defaultOption->defaultValue;
    }
    throw RuntimeException{"Invalid option name: " + lowerCaseOptionName + "."};
}

transaction::Transaction* ClientContext::getTx() const {
    return transactionContext->getActiveTransaction();
}

TransactionContext* ClientContext::getTransactionContext() const {
    return transactionContext.get();
}

common::ProgressBar* ClientContext::getProgressBar() const {
    return progressBar.get();
}

void ClientContext::addScanReplace(function::ScanReplacement scanReplacement) {
    scanReplacements.push_back(std::move(scanReplacement));
}

std::unique_ptr<function::ScanReplacementData> ClientContext::tryReplace(
    const std::string& objectName) const {
    for (auto& scanReplacement : scanReplacements) {
        auto replaceData = scanReplacement.replaceFunc(objectName);
        if (replaceData == nullptr) {
            continue; // Fail to replace.
        }
        return replaceData;
    }
    return nullptr;
}

void ClientContext::setExtensionOption(std::string name, common::Value value) {
    StringUtils::toLower(name);
    extensionOptionValues.insert_or_assign(name, std::move(value));
}

extension::ExtensionOptions* ClientContext::getExtensionOptions() const {
    return localDatabase->extensionOptions.get();
}

std::string ClientContext::getExtensionDir() const {
    return common::stringFormat("{}/.kuzu/extension/{}/{}", clientConfig.homeDirectory,
        KUZU_EXTENSION_VERSION, kuzu::extension::getPlatform());
}

std::string ClientContext::getDatabasePath() const {
    return localDatabase->databasePath;
}

DatabaseManager* ClientContext::getDatabaseManager() const {
    return localDatabase->databaseManager.get();
}

storage::StorageManager* ClientContext::getStorageManager() const {
    if (remoteDatabase == nullptr) {
        return localDatabase->storageManager.get();
    } else {
        return remoteDatabase->getStorageManager();
    }
}

storage::MemoryManager* ClientContext::getMemoryManager() {
    return localDatabase->memoryManager.get();
}

catalog::Catalog* ClientContext::getCatalog() const {
    if (remoteDatabase == nullptr) {
        return localDatabase->catalog.get();
    } else {
        return remoteDatabase->getCatalog();
    }
}

transaction::TransactionManager* ClientContext::getTransactionManagerUnsafe() const {
    if (remoteDatabase == nullptr) {
        return localDatabase->transactionManager.get();
    } else {
        return remoteDatabase->getTransactionManager();
    }
}

VirtualFileSystem* ClientContext::getVFSUnsafe() const {
    return localDatabase->vfs.get();
}

common::RandomEngine* ClientContext::getRandomEngine() {
    return randomEngine.get();
}

std::string ClientContext::getEnvVariable(const std::string& name) {
#if defined(_WIN32)
    auto envValue = common::WindowsUtils::utf8ToUnicode(name.c_str());
    auto result = _wgetenv(envValue.c_str());
    if (!result) {
        return std::string();
    }
    return WindowsUtils::unicodeToUTF8(result);
#else
    const char* env = getenv(name.c_str()); // NOLINT(*-mt-unsafe)
    if (!env) {
        return std::string();
    }
    return env;
#endif
}

std::unique_ptr<PreparedStatement> ClientContext::prepare(std::string_view query) {
    auto preparedStatement = std::unique_ptr<PreparedStatement>();
    if (query.empty()) {
        return preparedStatementWithError("Connection Exception: Query is empty.");
    }
    std::unique_lock<std::mutex> lck{mtx};
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = Parser::parseQuery(query);
    } catch (std::exception& exception) {
        return preparedStatementWithError(exception.what());
    }
    if (parsedStatements.size() > 1) {
        return preparedStatementWithError(
            "Connection Exception: We do not support prepare multiple statements.");
    }
    return prepareNoLock(parsedStatements[0]);
}

std::unique_ptr<PreparedStatement> ClientContext::prepareTest(std::string_view query) {
    auto preparedStatement = std::unique_ptr<PreparedStatement>();
    std::unique_lock<std::mutex> lck{mtx};
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = Parser::parseQuery(query);
    } catch (std::exception& exception) {
        return preparedStatementWithError(exception.what());
    }
    if (parsedStatements.size() > 1) {
        return preparedStatementWithError(
            "Connection Exception: We do not support prepare multiple statements.");
    }
    if (parsedStatements.empty()) {
        return preparedStatementWithError("Connection Exception: Query is empty.");
    }
    return prepareNoLock(parsedStatements[0], false /* enumerate all plans */, "",
        false /*requireNewTx*/);
}

std::unique_ptr<QueryResult> ClientContext::query(std::string_view queryStatement) {
    return query(queryStatement, std::string_view() /*encodedJoin*/, false /*enumerateAllPlans */);
}

std::unique_ptr<QueryResult> ClientContext::query(std::string_view query,
    std::string_view encodedJoin, bool enumerateAllPlans) {
    lock_t lck{mtx};
    if (query.empty()) {
        return queryResultWithError("Connection Exception: Query is empty.");
    }
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = Parser::parseQuery(query);
    } catch (std::exception& exception) {
        return queryResultWithError(exception.what());
    }
    std::unique_ptr<QueryResult> queryResult;
    QueryResult* lastResult = nullptr;
    for (auto& statement : parsedStatements) {
        auto preparedStatement = prepareNoLock(statement,
            enumerateAllPlans /* enumerate all plans */, encodedJoin, false /*requireNewTx*/);
        auto currentQueryResult = executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), 0u,
            false /*requiredNexTx*/);
        if (!lastResult) {
            // first result of the query
            queryResult = std::move(currentQueryResult);
            lastResult = queryResult.get();
        } else {
            lastResult->nextQueryResult = std::move(currentQueryResult);
            lastResult = lastResult->nextQueryResult.get();
        }
    }
    return queryResult;
}

std::unique_ptr<QueryResult> ClientContext::queryResultWithError(std::string_view errMsg) {
    auto queryResult = std::make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    queryResult->nextQueryResult = nullptr;
    queryResult->queryResultIterator = QueryResult::QueryResultIterator{queryResult.get()};
    return queryResult;
}

std::unique_ptr<PreparedStatement> ClientContext::preparedStatementWithError(
    std::string_view errMsg) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    preparedStatement->success = false;
    preparedStatement->errMsg = errMsg;
    return preparedStatement;
}

std::unique_ptr<PreparedStatement> ClientContext::prepareNoLock(
    std::shared_ptr<Statement> parsedStatement, bool enumerateAllPlans,
    std::string_view encodedJoin, bool requireNewTx,
    std::optional<std::unordered_map<std::string, std::shared_ptr<common::Value>>> inputParams) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    try {
        preparedStatement->preparedSummary.statementType = parsedStatement->getStatementType();
        preparedStatement->readOnly =
            parser::StatementReadWriteAnalyzer().isReadOnly(*parsedStatement);
        if (!canExecuteWriteQuery() && !preparedStatement->isReadOnly()) {
            throw ConnectionException("Cannot execute write operations in a read-only database!");
        }
        preparedStatement->parsedStatement = parsedStatement;
        if (parsedStatement->requireTx()) {
            if (transactionContext->isAutoTransaction()) {
                transactionContext->beginAutoTransaction(preparedStatement->readOnly);
            } else {
                transactionContext->validateManualTransaction(preparedStatement->readOnly);
            }
            if (!this->getTx()->isReadOnly()) {
                if (this->remoteDatabase == nullptr) {
                    localDatabase->storageManager->initStatistics();
                } else {
                    remoteDatabase->getStorageManager()->initStatistics();
                }
            }
        }
        // binding
        auto binder = Binder(this);
        if (inputParams) {
            binder.setInputParameters(*inputParams);
        }
        auto boundStatement = binder.bind(*parsedStatement);
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->statementResult =
            std::make_unique<BoundStatementResult>(boundStatement->getStatementResult()->copy());
        // planning
        auto planner = Planner(this);
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        if (enumerateAllPlans) {
            plans = planner.getAllPlans(*boundStatement);
        } else {
            plans.push_back(planner.getBestPlan(*boundStatement));
        }
        // optimizing
        for (auto& plan : plans) {
            optimizer::Optimizer::optimize(plan.get(), this);
        }
        if (!encodedJoin.empty()) {
            std::unique_ptr<LogicalPlan> match;
            for (auto& plan : plans) {
                if (LogicalPlanUtil::encodeJoin(*plan) == encodedJoin) {
                    match = std::move(plan);
                }
            }
            if (match == nullptr) {
                throw ConnectionException(
                    stringFormat("Cannot find a plan matching {}", encodedJoin));
            }
            preparedStatement->logicalPlans.push_back(std::move(match));
        } else {
            preparedStatement->logicalPlans = std::move(plans);
        }
        if (transactionContext->isAutoTransaction() && requireNewTx) {
            this->transactionContext->commit();
        }
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
        this->transactionContext->rollback();
    }
    compilingTimer.stop();
    preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
    return preparedStatement;
}

std::vector<std::shared_ptr<Statement>> ClientContext::parseQuery(std::string_view query) {
    std::vector<std::shared_ptr<Statement>> statements;
    if (query.empty()) {
        return statements;
    }
    statements = Parser::parseQuery(query);
    return statements;
}

void ClientContext::setDefaultDatabase(AttachedKuzuDatabase* defaultDatabase_) {
    this->remoteDatabase = defaultDatabase_;
}

bool ClientContext::hasDefaultDatabase() {
    return this->remoteDatabase != nullptr;
}

std::unique_ptr<QueryResult> ClientContext::executeWithParams(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>>
        inputParams) { // NOLINT(performance-unnecessary-value-param): It doesn't make sense to pass
                       // the map as a const reference.
    lock_t lck{mtx};
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, inputParams);
    } catch (Exception& exception) {
        std::string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
    // rebind
    KU_ASSERT(preparedStatement->parsedStatement != nullptr);
    auto rebindPreparedStatement = prepareNoLock(preparedStatement->parsedStatement, false, "",
        false, preparedStatement->parameterMap);
    return executeAndAutoCommitIfNecessaryNoLock(rebindPreparedStatement.get(), 0u, false);
}

void ClientContext::bindParametersNoLock(PreparedStatement* preparedStatement,
    const std::unordered_map<std::string, std::unique_ptr<Value>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, value] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw Exception("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        // The much more natural `parameterMap.at(name) = std::move(v)` fails.
        // The reason is that other parts of the code rely on the existing Value object to be
        // modified in-place, not replaced in this map.
        *parameterMap.at(name) = std::move(*value);
    }
}

std::unique_ptr<QueryResult> ClientContext::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx, bool requiredNexTx) {
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    if (preparedStatement->parsedStatement->requireTx() && requiredNexTx && getTx() == nullptr) {
        this->transactionContext->beginAutoTransaction(preparedStatement->isReadOnly());
        if (!preparedStatement->readOnly) {
            if (remoteDatabase == nullptr) {
                localDatabase->storageManager->initStatistics();
            } else {
                remoteDatabase->getStorageManager()->initStatistics();
            }
        }
    }
    this->resetActiveQuery();
    this->startTimer();
    auto mapper = PlanMapper(this);
    std::unique_ptr<PhysicalPlan> physicalPlan;
    if (preparedStatement->isSuccess()) {
        try {
            physicalPlan =
                mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[planIdx].get(),
                    preparedStatement->statementResult->getColumns());
        } catch (std::exception& exception) {
            this->transactionContext->rollback();
            return queryResultWithError(exception.what());
        }
    }
    auto queryResult = std::make_unique<QueryResult>(preparedStatement->preparedSummary);
    auto profiler = std::make_unique<Profiler>();
    auto executionContext = std::make_unique<ExecutionContext>(profiler.get(), this);
    profiler->enabled = preparedStatement->isProfile();
    auto executingTimer = TimeMetric(true /* enable */);
    executingTimer.start();
    std::shared_ptr<FactorizedTable> resultFT;
    try {
        if (preparedStatement->isTransactionStatement()) {
            resultFT =
                localDatabase->queryProcessor->execute(physicalPlan.get(), executionContext.get());
        } else {
            if (this->transactionContext->isAutoTransaction()) {
                resultFT = localDatabase->queryProcessor->execute(physicalPlan.get(),
                    executionContext.get());
                this->transactionContext->commit();
            } else {
                resultFT = localDatabase->queryProcessor->execute(physicalPlan.get(),
                    executionContext.get());
            }
        }
    } catch (Exception& exception) {
        this->transactionContext->rollback();
        return queryResultWithError(std::string(exception.what()));
    }
    executingTimer.stop();
    queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
    queryResult->initResultTableAndIterator(std::move(resultFT),
        preparedStatement->statementResult->getColumns());
    return queryResult;
}

void ClientContext::addScalarFunction(std::string name, function::function_set definitions) {
    localDatabase->catalog->addFunction(getTx(), CatalogEntryType::SCALAR_FUNCTION_ENTRY,
        std::move(name), std::move(definitions));
}

bool ClientContext::startUDFAutoTrx(transaction::TransactionContext* trx) {
    if (!trx->hasActiveTransaction()) {
        auto res = query("BEGIN TRANSACTION");
        KU_ASSERT(res->isSuccess());
        return true;
    }
    return false;
}

void ClientContext::commitUDFTrx(bool isAutoCommitTrx) {
    if (isAutoCommitTrx) {
        auto res = query("COMMIT");
        KU_ASSERT(res->isSuccess());
    }
}

bool ClientContext::canExecuteWriteQuery() {
    if (dbConfig.readOnly) {
        return false;
    }
    // Note: we can only attach a remote kuzu database in read-only mode and only one
    // remote kuzu database can be attached.
    auto dbManager = localDatabase->databaseManager.get();
    for (auto& attachedDB : dbManager->getAttachedDatabases()) {
        if (attachedDB->getDBType() == common::ATTACHED_KUZU_DB_TYPE) {
            return false;
        }
    }
    return true;
}

void ClientContext::runQuery(std::string query) {
    // TODO(Jimain): this is special for "Import database". Should refactor after we support
    // multiple query statements in one Tx.
    // Currently, we split multiple query statements into single query and execute them one by one,
    // each with an auto transaction. The correct way is to execute them in one transaction. But we
    // do not support DDL and copy in one Tx.
    if (transactionContext->hasActiveTransaction()) {
        transactionContext->commit();
    }
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = Parser::parseQuery(query);
    } catch (std::exception& exception) {
        throw ConnectionException(exception.what());
    }
    if (parsedStatements.empty()) {
        throw ConnectionException("Connection Exception: Query is empty.");
    }
    try {
        for (auto& statement : parsedStatements) {
            auto preparedStatement = prepareNoLock(statement, false, "", false);
            auto currentQueryResult =
                executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), 0u, false);
            if (!currentQueryResult->isSuccess()) {
                throw ConnectionException(currentQueryResult->errMsg);
            }
        }
    } catch (std::exception& exception) {
        throw ConnectionException(exception.what());
    }
    return;
}
} // namespace main
} // namespace kuzu
