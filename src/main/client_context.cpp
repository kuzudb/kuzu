#include "main/client_context.h"

#include <utility>

#include "binder/binder.h"
#include "common/exception/connection.h"
#include "common/exception/runtime.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "main/database.h"
#include "main/db_config.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "parser/visitor/statement_read_write_analyzer.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/planner.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"
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

ClientContext::ClientContext(Database* database) : database{database} {
    progressBar = std::make_unique<common::ProgressBar>();
    transactionContext = std::make_unique<TransactionContext>(database);
    randomEngine = std::make_unique<common::RandomEngine>();
#if defined(_WIN32)
    config.homeDirectory = getEnvVariable("USERPROFILE");
#else
    config.homeDirectory = getEnvVariable("HOME");
#endif
    config.fileSearchPath = "";
    config.enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
    config.numThreads = database->systemConfig.maxNumThreads;
    config.timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
    config.varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
    config.enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
}

uint64_t ClientContext::getTimeoutRemainingInMS() const {
    KU_ASSERT(hasTimeout());
    auto elapsed = activeQuery.timer.getElapsedTimeInMS();
    return elapsed >= config.timeoutInMS ? 0 : config.timeoutInMS - elapsed;
}

void ClientContext::startTimer() {
    if (hasTimeout()) {
        activeQuery.timer.start();
    }
}

void ClientContext::setQueryTimeOut(uint64_t timeoutInMS) {
    lock_t lck{mtx};
    config.timeoutInMS = timeoutInMS;
}

uint64_t ClientContext::getQueryTimeOut() const {
    return config.timeoutInMS;
}

void ClientContext::setMaxNumThreadForExec(uint64_t numThreads) {
    lock_t lck{mtx};
    config.numThreads = numThreads;
}

uint64_t ClientContext::getMaxNumThreadForExec() const {
    return config.numThreads;
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
    // Lastly, find the default value in db config.
    auto defaultOption = database->extensionOptions->getExtensionOption(lowerCaseOptionName);
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

void ClientContext::setExtensionOption(std::string name, common::Value value) {
    StringUtils::toLower(name);
    extensionOptionValues.insert_or_assign(name, std::move(value));
}

extension::ExtensionOptions* ClientContext::getExtensionOptions() const {
    return database->extensionOptions.get();
}

std::string ClientContext::getExtensionDir() const {
    return common::stringFormat("{}/.kuzu/extension", config.homeDirectory);
}

storage::StorageManager* ClientContext::getStorageManager() {
    return database->storageManager.get();
}

storage::MemoryManager* ClientContext::getMemoryManager() {
    return database->memoryManager.get();
}

catalog::Catalog* ClientContext::getCatalog() {
    return database->catalog.get();
}

VirtualFileSystem* ClientContext::getVFSUnsafe() const {
    return database->vfs.get();
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
    std::unique_lock<std::mutex> lck{mtx};
    auto parsedStatements = std::vector<std::unique_ptr<Statement>>();
    try {
        parsedStatements = parseQuery(query);
    } catch (std::exception& exception) { return preparedStatementWithError(exception.what()); }
    if (parsedStatements.size() > 1) {
        return preparedStatementWithError(
            "Connection Exception: We do not support prepare multiple statements.");
    }
    if (parsedStatements.empty()) {
        return preparedStatementWithError("Connection Exception: Query is empty.");
    }
    return prepareNoLock(parsedStatements[0].get());
}

std::unique_ptr<QueryResult> ClientContext::query(std::string_view queryStatement) {
    return query(queryStatement, std::string_view() /*encodedJoin*/, false /*enumerateAllPlans */);
}

std::unique_ptr<QueryResult> ClientContext::query(
    std::string_view query, std::string_view encodedJoin, bool enumerateAllPlans) {
    lock_t lck{mtx};
    // parsing
    auto parsedStatements = std::vector<std::unique_ptr<Statement>>();
    try {
        parsedStatements = parseQuery(query);
    } catch (std::exception& exception) { return queryResultWithError(exception.what()); }
    if (parsedStatements.empty()) {
        return queryResultWithError("Connection Exception: Query is empty.");
    }
    std::unique_ptr<QueryResult> queryResult;
    QueryResult* lastResult = nullptr;
    for (auto& statement : parsedStatements) {
        auto preparedStatement = prepareNoLock(
            statement.get(), enumerateAllPlans /* enumerate all plans */, encodedJoin);
        auto currentQueryResult = executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
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
    Statement* parsedStatement, bool enumerateAllPlans, std::string_view encodedJoin) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    try {
        preparedStatement->preparedSummary.statementType = parsedStatement->getStatementType();
        preparedStatement->readOnly =
            parser::StatementReadWriteAnalyzer().isReadOnly(*parsedStatement);
        if (database->systemConfig.readOnly && !preparedStatement->isReadOnly()) {
            throw ConnectionException("Cannot execute write operations in a read-only database!");
        }
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
        compilingTimer.stop();
        preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
        return preparedStatement;
    }
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<LogicalPlan> logicalPlan;
    try {
        // parsing
        if (parsedStatement->getStatementType() != StatementType::TRANSACTION) {
            if (transactionContext->isAutoTransaction()) {
                transactionContext->beginAutoTransaction(preparedStatement->readOnly);
            } else {
                transactionContext->validateManualTransaction(
                    preparedStatement->allowActiveTransaction(), preparedStatement->readOnly);
            }
            if (!this->getTx()->isReadOnly()) {
                database->catalog->initCatalogContentForWriteTrxIfNecessary();
                database->storageManager->initStatistics();
            }
        }
        // binding
        auto binder = Binder(this);
        auto boundStatement = binder.bind(*parsedStatement);
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->statementResult =
            std::make_unique<BoundStatementResult>(boundStatement->getStatementResult()->copy());
        // planning
        auto planner = Planner(database->catalog.get(), database->storageManager.get());
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
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
        this->transactionContext->rollback();
    }
    compilingTimer.stop();
    preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
    return preparedStatement;
}

std::vector<std::unique_ptr<Statement>> ClientContext::parseQuery(std::string_view query) {
    std::vector<std::unique_ptr<Statement>> statements;
    if (query.empty()) {
        return statements;
    }
    statements = Parser::parseQuery(query);
    return statements;
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
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement);
}

void ClientContext::bindParametersNoLock(PreparedStatement* preparedStatement,
    const std::unordered_map<std::string, std::unique_ptr<Value>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, value] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw Exception("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        if (value->getDataType()->getLogicalTypeID() == LogicalTypeID::ANY) {
            value->setDataType(*expectParam->getDataType());
        }
        if (*expectParam->getDataType() != *value->getDataType()) {
            throw Exception("Parameter " + name + " has data type " +
                            value->getDataType()->toString() + " but expects " +
                            expectParam->getDataType()->toString() + ".");
        }
        // The much more natural `parameterMap.at(name) = std::move(v)` fails.
        // The reason is that other parts of the code rely on the existing Value object to be
        // modified in-place, not replaced in this map.
        *parameterMap.at(name) = std::move(*value);
    }
}

std::unique_ptr<QueryResult> ClientContext::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx) {
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    if (preparedStatement->preparedSummary.statementType != common::StatementType::TRANSACTION &&
        this->getTx() == nullptr) {
        this->transactionContext->beginAutoTransaction(preparedStatement->isReadOnly());
        if (!preparedStatement->readOnly) {
            database->catalog->initCatalogContentForWriteTrxIfNecessary();
            database->storageManager->initStatistics();
        }
    }
    this->resetActiveQuery();
    this->startTimer();
    auto mapper = PlanMapper(
        *database->storageManager, database->memoryManager.get(), database->catalog.get(), this);
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
                database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
        } else {
            if (this->transactionContext->isAutoTransaction()) {
                resultFT =
                    database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
                this->transactionContext->commit();
            } else {
                resultFT =
                    database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
            }
        }
    } catch (Exception& exception) {
        this->transactionContext->rollback();
        return queryResultWithError(std::string(exception.what()));
    }
    executingTimer.stop();
    queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
    queryResult->initResultTableAndIterator(
        std::move(resultFT), preparedStatement->statementResult->getColumns());
    return queryResult;
}

void ClientContext::addScalarFunction(std::string name, function::function_set definitions) {
    database->catalog->addFunction(std::move(name), std::move(definitions));
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

void ClientContext::runQuery(std::string query) {
    // TODO(Jimain): this is special for "Import database". Should refactor after we support
    // multiple query statements in one Tx.
    // Currently, we split multiple query statements into single query and execute them one by one,
    // each with an auto transaction. The correct way is to execute them in one transaction. But we
    // do not support DDL and copy in one Tx.
    if (transactionContext->hasActiveTransaction()) {
        transactionContext->commit();
    }
    auto parsedStatements = std::vector<std::unique_ptr<Statement>>();
    try {
        parsedStatements = parseQuery(query);
    } catch (std::exception& exception) { throw ConnectionException(exception.what()); }
    if (parsedStatements.empty()) {
        throw ConnectionException("Connection Exception: Query is empty.");
    }
    try {
        for (auto& statement : parsedStatements) {
            auto preparedStatement = prepareNoLock(statement.get());
            auto currentQueryResult =
                executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
            if (!currentQueryResult->isSuccess()) {
                throw ConnectionException(currentQueryResult->errMsg);
            }
        }
    } catch (std::exception& exception) { throw ConnectionException(exception.what()); }
    return;
}
} // namespace main
} // namespace kuzu
