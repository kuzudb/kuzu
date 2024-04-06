#include "main/connection.h"

#include <utility>

#include "common/random_engine.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

Connection::Connection(Database* database) {
    KU_ASSERT(database != nullptr);
    this->database = database;
    clientContext = std::make_unique<ClientContext>(database);
}

Connection::~Connection() {}

void Connection::setMaxNumThreadForExec(uint64_t numThreads) {
    clientContext->setMaxNumThreadForExec(numThreads);
}

uint64_t Connection::getMaxNumThreadForExec() {
    return clientContext->getMaxNumThreadForExec();
}

std::unique_ptr<PreparedStatement> Connection::prepare(std::string_view query) {
    return clientContext->prepare(query);
}

std::unique_ptr<QueryResult> Connection::query(std::string_view queryStatement) {
    return clientContext->query(queryStatement);
}

std::unique_ptr<QueryResult> Connection::query(std::string_view query, std::string_view encodedJoin,
    bool enumerateAllPlans) {
    return clientContext->query(query, encodedJoin, enumerateAllPlans);
}

std::unique_ptr<QueryResult> Connection::queryResultWithError(std::string_view errMsg) {
    return clientContext->queryResultWithError(errMsg);
}

std::unique_ptr<PreparedStatement> Connection::preparedStatementWithError(std::string_view errMsg) {
    return clientContext->preparedStatementWithError(errMsg);
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(
    std::shared_ptr<Statement> parsedStatement, bool enumerateAllPlans,
    std::string_view encodedJoin) {
    return clientContext->prepareNoLock(parsedStatement, enumerateAllPlans, encodedJoin);
}

void Connection::interrupt() {
    clientContext->interrupt();
}

void Connection::setQueryTimeOut(uint64_t timeoutInMS) {
    clientContext->setQueryTimeOut(timeoutInMS);
}

uint64_t Connection::getQueryTimeOut() {
    return clientContext->getQueryTimeOut();
}

std::unique_ptr<QueryResult> Connection::executeWithParams(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams) {
    return clientContext->executeWithParams(preparedStatement, std::move(inputParams));
}

void Connection::bindParametersNoLock(PreparedStatement* preparedStatement,
    const std::unordered_map<std::string, std::unique_ptr<Value>>& inputParams) {
    return clientContext->bindParametersNoLock(preparedStatement, inputParams);
}

std::unique_ptr<QueryResult> Connection::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx) {
    return clientContext->executeAndAutoCommitIfNecessaryNoLock(preparedStatement, planIdx);
}

void Connection::addScalarFunction(std::string name, function::function_set definitions) {
    clientContext->addScalarFunction(name, std::move(definitions));
}

bool Connection::startUDFAutoTrx(transaction::TransactionContext* trx) {
    return clientContext->startUDFAutoTrx(trx);
}

void Connection::commitUDFTrx(bool isAutoCommitTrx) {
    return clientContext->commitUDFTrx(isAutoCommitTrx);
}

} // namespace main
} // namespace kuzu
