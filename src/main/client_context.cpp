#include "main/client_context.h"

#include "binder/binder.h"
#include "common/exception/checkpoint.h"
#include "common/exception/connection.h"
#include "common/exception/runtime.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "common/task_system/progress_bar.h"
#include "extension/extension.h"
#include "extension/extension_manager.h"
#include "graph/graph_entry.h"
#include "main/attached_database.h"
#include "main/database.h"
#include "main/database_manager.h"
#include "main/db_config.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "parser/visitor/standalone_call_rewriter.h"
#include "parser/visitor/statement_read_write_analyzer.h"
#include "planner/planner.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/spiller.h"
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
    : dbConfig{database->dbConfig}, localDatabase{database}, warningContext(&clientConfig) {
    transactionContext = std::make_unique<TransactionContext>(*this);
    randomEngine = std::make_unique<RandomEngine>();
    remoteDatabase = nullptr;
    graphEntrySet = std::make_unique<graph::GraphEntrySet>();
#if defined(_WIN32)
    clientConfig.homeDirectory = getEnvVariable("USERPROFILE");
#else
    clientConfig.homeDirectory = getEnvVariable("HOME");
#endif
    clientConfig.fileSearchPath = "";
    clientConfig.enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
    clientConfig.enableZoneMap = ClientConfigDefault::ENABLE_ZONE_MAP;
    clientConfig.numThreads = database->dbConfig.maxNumThreads;
    clientConfig.timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
    clientConfig.varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
    clientConfig.enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
    clientConfig.showProgressAfter = ClientConfigDefault::SHOW_PROGRESS_AFTER;
    clientConfig.recursivePatternSemantic = ClientConfigDefault::RECURSIVE_PATTERN_SEMANTIC;
    clientConfig.recursivePatternCardinalityScaleFactor =
        ClientConfigDefault::RECURSIVE_PATTERN_FACTOR;
    clientConfig.disableMapKeyCheck = ClientConfigDefault::DISABLE_MAP_KEY_CHECK;
    clientConfig.warningLimit = ClientConfigDefault::WARNING_LIMIT;
    progressBar = std::make_unique<ProgressBar>(clientConfig.enableProgressBar);
}

ClientContext::~ClientContext() = default;

uint64_t ClientContext::getTimeoutRemainingInMS() const {
    KU_ASSERT(hasTimeout());
    const auto elapsed = activeQuery.timer.getElapsedTimeInMS();
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
    if (numThreads == 0) {
        numThreads = localDatabase->dbConfig.maxNumThreads;
    }
    clientConfig.numThreads = numThreads;
}

uint64_t ClientContext::getMaxNumThreadForExec() const {
    return clientConfig.numThreads;
}

Value ClientContext::getCurrentSetting(const std::string& optionName) const {
    auto lowerCaseOptionName = optionName;
    StringUtils::toLower(lowerCaseOptionName);
    // Firstly, try to find in built-in options.
    const auto option = DBConfig::getOptionByName(lowerCaseOptionName);
    if (option != nullptr) {
        return option->getSetting(this);
    }
    // Secondly, try to find in current client session.
    if (extensionOptionValues.contains(lowerCaseOptionName)) {
        return extensionOptionValues.at(lowerCaseOptionName);
    }
    // Lastly, find the default value in db clientConfig.
    const auto defaultOption = getExtensionOption(lowerCaseOptionName);
    if (defaultOption != nullptr) {
        return defaultOption->defaultValue;
    }
    throw RuntimeException{"Invalid option name: " + lowerCaseOptionName + "."};
}

Transaction* ClientContext::getTransaction() const {
    return transactionContext->getActiveTransaction();
}

TransactionContext* ClientContext::getTransactionContext() const {
    return transactionContext.get();
}

ProgressBar* ClientContext::getProgressBar() const {
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

void ClientContext::setExtensionOption(std::string name, Value value) {
    StringUtils::toLower(name);
    extensionOptionValues.insert_or_assign(name, std::move(value));
}

const main::ExtensionOption* ClientContext::getExtensionOption(std::string optionName) const {
    return localDatabase->extensionManager->getExtensionOption(optionName);
}

std::string ClientContext::getExtensionDir() const {
    return stringFormat("{}/.kuzu/extension/{}/{}/", clientConfig.homeDirectory,
        KUZU_EXTENSION_VERSION, extension::getPlatform());
}

std::string ClientContext::getDatabasePath() const {
    return localDatabase->databasePath;
}

TaskScheduler* ClientContext::getTaskScheduler() const {
    return localDatabase->queryProcessor->getTaskScheduler();
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

storage::MemoryManager* ClientContext::getMemoryManager() const {
    return localDatabase->memoryManager.get();
}

extension::ExtensionManager* ClientContext::getExtensionManager() const {
    return localDatabase->extensionManager.get();
}

storage::WAL* ClientContext::getWAL() const {
    KU_ASSERT(localDatabase && localDatabase->storageManager);
    return &localDatabase->storageManager->getWAL();
}

Catalog* ClientContext::getCatalog() const {
    if (remoteDatabase == nullptr) {
        return localDatabase->catalog.get();
    } else {
        return remoteDatabase->getCatalog();
    }
}

TransactionManager* ClientContext::getTransactionManagerUnsafe() const {
    if (remoteDatabase == nullptr) {
        return localDatabase->transactionManager.get();
    } else {
        return remoteDatabase->getTransactionManager();
    }
}

VirtualFileSystem* ClientContext::getVFSUnsafe() const {
    return localDatabase->vfs.get();
}

RandomEngine* ClientContext::getRandomEngine() const {
    return randomEngine.get();
}

std::string ClientContext::getEnvVariable(const std::string& name) {
#if defined(_WIN32)
    auto envValue = WindowsUtils::utf8ToUnicode(name.c_str());
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

void ClientContext::setDefaultDatabase(AttachedKuzuDatabase* defaultDatabase_) {
    remoteDatabase = defaultDatabase_;
}

bool ClientContext::hasDefaultDatabase() const {
    return remoteDatabase != nullptr;
}

void ClientContext::addScalarFunction(std::string name, function::function_set definitions) {
    TransactionHelper::runFuncInTransaction(
        *transactionContext,
        [&]() {
            localDatabase->catalog->addFunction(getTransaction(),
                CatalogEntryType::SCALAR_FUNCTION_ENTRY, std::move(name), std::move(definitions));
        },
        false /*readOnlyStatement*/, false /*isTransactionStatement*/,
        TransactionHelper::TransactionCommitAction::COMMIT_IF_NEW);
}

void ClientContext::removeScalarFunction(const std::string& name) {
    TransactionHelper::runFuncInTransaction(
        *transactionContext,
        [&]() { localDatabase->catalog->dropFunction(getTransaction(), name); },
        false /*readOnlyStatement*/, false /*isTransactionStatement*/,
        TransactionHelper::TransactionCommitAction::COMMIT_IF_NEW);
}

WarningContext& ClientContext::getWarningContextUnsafe() {
    return warningContext;
}

const WarningContext& ClientContext::getWarningContext() const {
    return warningContext;
}

graph::GraphEntrySet& ClientContext::getGraphEntrySetUnsafe() {
    return *graphEntrySet;
}

void ClientContext::cleanUp() {
    getVFSUnsafe()->cleanUP(this);
}

std::unique_ptr<PreparedStatement> ClientContext::prepare(std::string_view query) {
    std::unique_lock lck{mtx};
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = parseQuery(query);
    } catch (std::exception& exception) {
        return preparedStatementWithError(exception.what());
    }
    if (parsedStatements.size() > 1) {
        return preparedStatementWithError(
            "Connection Exception: We do not support prepare multiple statements.");
    }
    auto result = prepareNoLock(parsedStatements[0], true /*shouldCommitNewTransaction*/);
    useInternalCatalogEntry_ = false;
    return result;
}

std::unique_ptr<QueryResult> ClientContext::executeWithParams(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams,
    std::optional<uint64_t> queryID) { // NOLINT(performance-unnecessary-value-param): It doesn't
    // make sense to pass the map as a const reference.
    lock_t lck{mtx};
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, inputParams);
    } catch (std::exception& e) {
        return queryResultWithError(e.what());
    }
    // rebind
    KU_ASSERT(preparedStatement->parsedStatement != nullptr);
    const auto rebindPreparedStatement = prepareNoLock(preparedStatement->parsedStatement,
        false /*shouldCommitNewTransaction*/, preparedStatement->parameterMap);
    useInternalCatalogEntry_ = false;
    return executeNoLock(rebindPreparedStatement.get(), queryID);
}

std::unique_ptr<QueryResult> ClientContext::query(std::string_view queryStatement,
    std::optional<uint64_t> queryID) {
    lock_t lck{mtx};
    return queryNoLock(queryStatement, queryID);
}

std::unique_ptr<QueryResult> ClientContext::queryNoLock(std::string_view query,
    std::optional<uint64_t> queryID) {
    auto parsedStatements = std::vector<std::shared_ptr<Statement>>();
    try {
        parsedStatements = parseQuery(query);
    } catch (std::exception& exception) {
        return queryResultWithError(exception.what());
    }
    std::unique_ptr<QueryResult> queryResult;
    QueryResult* lastResult = nullptr;
    double internalCompilingTime = 0.0, internalExecutionTime = 0.0;
    for (const auto& statement : parsedStatements) {
        auto preparedStatement = prepareNoLock(statement, false /*shouldCommitNewTransaction*/);
        auto currentQueryResult = executeNoLock(preparedStatement.get(), queryID);
        if (!currentQueryResult->isSuccess()) {
            if (!lastResult) {
                queryResult = std::move(currentQueryResult);
            } else {
                queryResult->nextQueryResult = std::move(currentQueryResult);
            }
            break;
        }
        auto currentQuerySummary = currentQueryResult->getQuerySummary();
        if (statement->isInternal()) {
            // The result of internal statements should be invisible to end users. Skip chaining the
            // result of internal statements to the final result to end users.
            internalCompilingTime += currentQuerySummary->getCompilingTime();
            internalExecutionTime += currentQuerySummary->getExecutionTime();
            continue;
        }
        currentQuerySummary->incrementCompilingTime(internalCompilingTime);
        currentQuerySummary->incrementExecutionTime(internalExecutionTime);
        if (!lastResult) {
            // first result of the query
            queryResult = std::move(currentQueryResult);
            lastResult = queryResult.get();
        } else {
            lastResult->nextQueryResult = std::move(currentQueryResult);
            lastResult = lastResult->nextQueryResult.get();
        }
    }
    useInternalCatalogEntry_ = false;
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

void ClientContext::bindParametersNoLock(const PreparedStatement* preparedStatement,
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

std::vector<std::shared_ptr<Statement>> ClientContext::parseQuery(std::string_view query) {
    if (query.empty()) {
        throw ConnectionException("Query is empty.");
    }
    std::vector<std::shared_ptr<Statement>> statements;
    auto parserTimer = TimeMetric(true /*enable*/);
    parserTimer.start();
    auto parsedStatements = Parser::parseQuery(query);
    parserTimer.stop();
    const auto avgParsingTime = parserTimer.getElapsedTimeMS() / parsedStatements.size() / 1.0;
    StandaloneCallRewriter standaloneCallAnalyzer{this};
    for (auto i = 0u; i < parsedStatements.size(); i++) {
        auto rewriteQuery = standaloneCallAnalyzer.getRewriteQuery(*parsedStatements[i]);
        if (rewriteQuery.empty()) {
            parsedStatements[i]->setParsingTime(avgParsingTime);
            statements.push_back(std::move(parsedStatements[i]));
        } else {
            parserTimer.start();
            auto rewrittenStatements = Parser::parseQuery(rewriteQuery);
            parserTimer.stop();
            const auto avgRewriteParsingTime =
                parserTimer.getElapsedTimeMS() / rewrittenStatements.size() / 1.0;
            KU_ASSERT(rewrittenStatements.size() >= 1);
            for (auto j = 0u; j < rewrittenStatements.size() - 1; j++) {
                rewrittenStatements[j]->setParsingTime(avgParsingTime + avgRewriteParsingTime);
                rewrittenStatements[j]->setToInternal();
                statements.push_back(std::move(rewrittenStatements[j]));
            }
            auto lastRewrittenStatement = rewrittenStatements.back();
            lastRewrittenStatement->setParsingTime(avgParsingTime + avgRewriteParsingTime);
            statements.push_back(std::move(lastRewrittenStatement));
        }
    }
    return statements;
}

void ClientContext::validateTransaction(const PreparedStatement& preparedStatement) const {
    if (!canExecuteWriteQuery() && !preparedStatement.isReadOnly()) {
        throw ConnectionException("Cannot execute write operations in a read-only database!");
    }
    if (preparedStatement.parsedStatement->requireTransaction() &&
        transactionContext->hasActiveTransaction()) {
        KU_ASSERT(!transactionContext->isAutoTransaction());
        transactionContext->validateManualTransaction(preparedStatement.readOnly);
    }
}

std::unique_ptr<PreparedStatement> ClientContext::prepareNoLock(
    std::shared_ptr<Statement> parsedStatement, bool shouldCommitNewTransaction,
    std::optional<std::unordered_map<std::string, std::shared_ptr<Value>>> inputParams) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    auto prepareTimer = TimeMetric(true /* enable */);
    prepareTimer.start();
    try {
        preparedStatement->preparedSummary.statementType = parsedStatement->getStatementType();
        auto readWriteAnalyzer = StatementReadWriteAnalyzer(this);
        TransactionHelper::runFuncInTransaction(
            *transactionContext, [&]() -> void { readWriteAnalyzer.visit(*parsedStatement); },
            true /* readOnly */, false /* */,
            TransactionHelper::TransactionCommitAction::COMMIT_IF_NEW);
        preparedStatement->readOnly = readWriteAnalyzer.isReadOnly();
        preparedStatement->parsedStatement = std::move(parsedStatement);
        validateTransaction(*preparedStatement);

        TransactionHelper::runFuncInTransaction(
            *transactionContext,
            [&]() -> void {
                auto binder = Binder(this);
                if (inputParams) {
                    binder.setInputParameters(*inputParams);
                }
                const auto boundStatement = binder.bind(*preparedStatement->parsedStatement);
                preparedStatement->parameterMap = binder.getParameterMap();
                preparedStatement->statementResult = std::make_unique<BoundStatementResult>(
                    boundStatement->getStatementResult()->copy());
                // planning
                auto planner = Planner(this);
                auto bestPlan = planner.getBestPlan(*boundStatement);
                // optimizing
                optimizer::Optimizer::optimize(bestPlan.get(), this,
                    planner.getCardinalityEstimator());
                preparedStatement->logicalPlan = std::move(bestPlan);
            },
            preparedStatement->isReadOnly(), preparedStatement->isTransactionStatement(),
            TransactionHelper::getAction(shouldCommitNewTransaction,
                false /*shouldCommitAutoTransaction*/));
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
    }
    preparedStatement->useInternalCatalogEntry = useInternalCatalogEntry_;
    prepareTimer.stop();
    preparedStatement->preparedSummary.compilingTime =
        preparedStatement->parsedStatement->getParsingTime() + prepareTimer.getElapsedTimeMS();
    return preparedStatement;
}

std::unique_ptr<QueryResult> ClientContext::executeNoLock(PreparedStatement* preparedStatement,
    std::optional<uint64_t> queryID) {
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    useInternalCatalogEntry_ = preparedStatement->useInternalCatalogEntry;
    this->resetActiveQuery();
    this->startTimer();
    auto executingTimer = TimeMetric(true /* enable */);
    executingTimer.start();
    std::shared_ptr<FactorizedTable> resultFT;
    std::unique_ptr<QueryResult> queryResult;
    try {
        TransactionHelper::runFuncInTransaction(
            *transactionContext,
            [&]() -> void {
                const auto profiler = std::make_unique<Profiler>();
                profiler->enabled = preparedStatement->isProfile();
                if (!queryID) {
                    queryID = localDatabase->getNextQueryID();
                }
                const auto executionContext =
                    std::make_unique<ExecutionContext>(profiler.get(), this, *queryID);
                auto mapper = PlanMapper(executionContext.get());
                const auto physicalPlan =
                    mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get(),
                        preparedStatement->statementResult->getColumns());
                queryResult = std::make_unique<QueryResult>(preparedStatement->preparedSummary);
                if (preparedStatement->isTransactionStatement()) {
                    resultFT = localDatabase->queryProcessor->execute(physicalPlan.get(),
                        executionContext.get());
                } else {
                    getTransaction()->checkForceCheckpoint(preparedStatement->getStatementType());
                    resultFT = localDatabase->queryProcessor->execute(physicalPlan.get(),
                        executionContext.get());
                }
            },
            preparedStatement->isReadOnly(), preparedStatement->isTransactionStatement(),
            TransactionHelper::getAction(true /*shouldCommitNewTransaction*/,
                !preparedStatement->isTransactionStatement() /*shouldCommitAutoTransaction*/));
    } catch (std::exception& e) {
        useInternalCatalogEntry_ = false;
        return handleFailedExecution(queryID, e);
    }
    getMemoryManager()->getBufferManager()->getSpillerOrSkip(
        [](auto& spiller) { spiller.clearFile(); });
    executingTimer.stop();
    queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
    const auto sResult = preparedStatement->statementResult.get();
    queryResult->setColumnHeader(sResult->getColumnNames(), sResult->getColumnTypes());
    queryResult->initResultTableAndIterator(std::move(resultFT));
    return queryResult;
}

std::unique_ptr<QueryResult> ClientContext::handleFailedExecution(std::optional<uint64_t> queryID,
    const std::exception& e) const {
    getMemoryManager()->getBufferManager()->getSpillerOrSkip(
        [](auto& spiller) { spiller.clearFile(); });
    if (queryID.has_value()) {
        progressBar->endProgress(queryID.value());
    }
    return queryResultWithError(e.what());
}

ClientContext::TransactionHelper::TransactionCommitAction
ClientContext::TransactionHelper::getAction(bool commitIfNew, bool commitIfAuto) {
    if (commitIfNew && commitIfAuto) {
        return TransactionCommitAction::COMMIT_NEW_OR_AUTO;
    }
    if (commitIfNew) {
        return TransactionCommitAction::COMMIT_IF_NEW;
    }
    if (commitIfAuto) {
        return TransactionCommitAction::COMMIT_IF_AUTO;
    }
    return TransactionCommitAction::NOT_COMMIT;
}

// If there is an active transaction in the context, we execute the function in current active
// transaction. If there is no active transaction, we start an auto commit transaction.
void ClientContext::TransactionHelper::runFuncInTransaction(TransactionContext& context,
    const std::function<void()>& fun, bool readOnlyStatement, bool isTransactionStatement,
    TransactionCommitAction action) {
    KU_ASSERT(context.isAutoTransaction() || context.hasActiveTransaction());
    const bool requireNewTransaction =
        context.isAutoTransaction() && !context.hasActiveTransaction() && !isTransactionStatement;
    if (requireNewTransaction) {
        context.beginAutoTransaction(readOnlyStatement);
    }
    try {
        fun();
        if ((requireNewTransaction && commitIfNew(action)) ||
            (context.isAutoTransaction() && commitIfAuto(action))) {
            context.commit();
        }
    } catch (CheckpointException&) {
        context.clearTransaction();
        throw;
    } catch (std::exception&) {
        context.rollback();
        throw;
    }
}

bool ClientContext::canExecuteWriteQuery() const {
    if (dbConfig.readOnly) {
        return false;
    }
    // Note: we can only attach a remote kuzu database in read-only mode and only one
    // remote kuzu database can be attached.
    const auto dbManager = localDatabase->databaseManager.get();
    for (const auto& attachedDB : dbManager->getAttachedDatabases()) {
        if (attachedDB->getDBType() == common::ATTACHED_KUZU_DB_TYPE) {
            return false;
        }
    }
    return true;
}

} // namespace main
} // namespace kuzu
