#include "main/connection.h"

#include <utility>

#include "common/random_engine.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

Connection::Connection(Database* database) {
    KU_ASSERT(database != nullptr);
    this->database = database;
    this->dbLifeCycleManager = database->dbLifeCycleManager;
    clientContext = std::make_unique<ClientContext>(database);
}

Connection::~Connection() {
    clientContext->preventTransactionRollbackOnDestruction = dbLifeCycleManager->isDatabaseClosed;
}

void Connection::setMaxNumThreadForExec(uint64_t numThreads) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    clientContext->setMaxNumThreadForExec(numThreads);
}

uint64_t Connection::getMaxNumThreadForExec() {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    return clientContext->getMaxNumThreadForExec();
}

std::unique_ptr<PreparedStatement> Connection::prepare(std::string_view query) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    return clientContext->prepareWithParams(query);
}

std::unique_ptr<PreparedStatement> Connection::prepareWithParams(std::string_view query,
    std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    return clientContext->prepareWithParams(query, std::move(inputParams));
}

std::unique_ptr<QueryResult> Connection::query(std::string_view queryStatement) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult = clientContext->query(queryStatement);
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

std::unique_ptr<QueryResult> Connection::queryWithID(std::string_view queryStatement,
    uint64_t queryID) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult = clientContext->query(queryStatement, queryID);
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

std::unique_ptr<QueryResult> Connection::queryResultWithError(std::string_view errMsg) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult = clientContext->queryResultWithError(errMsg);
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

std::unique_ptr<PreparedStatement> Connection::preparedStatementWithError(std::string_view errMsg) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    return clientContext->preparedStatementWithError(errMsg);
}

void Connection::interrupt() {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    clientContext->interrupt();
}

void Connection::setQueryTimeOut(uint64_t timeoutInMS) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    clientContext->setQueryTimeOut(timeoutInMS);
}

std::unique_ptr<QueryResult> Connection::executeWithParams(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult = clientContext->executeWithParams(preparedStatement, std::move(inputParams));
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

std::unique_ptr<QueryResult> Connection::executeWithParamsWithID(
    PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams, uint64_t queryID) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult =
        clientContext->executeWithParams(preparedStatement, std::move(inputParams), queryID);
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

void Connection::bindParametersNoLock(PreparedStatement* preparedStatement,
    const std::unordered_map<std::string, std::unique_ptr<Value>>& inputParams) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    return clientContext->bindParametersNoLock(preparedStatement, inputParams);
}

std::unique_ptr<QueryResult> Connection::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    auto queryResult = clientContext->executeNoLock(preparedStatement, planIdx);
    queryResult->dbLifeCycleManager = dbLifeCycleManager;
    return queryResult;
}

void Connection::addScalarFunction(std::string name, function::function_set definitions) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    clientContext->addScalarFunction(name, std::move(definitions));
}

void Connection::removeScalarFunction(std::string name) {
    dbLifeCycleManager->checkDatabaseClosedOrThrow();
    clientContext->removeScalarFunction(name);
}

} // namespace main
} // namespace kuzu
