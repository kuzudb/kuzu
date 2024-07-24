#include "main/prepared_statement.h"

#include "binder/bound_statement_result.h" // IWYU pragma: keep (used to avoid error in destructor)
#include "common/enums/statement_type.h"
#include "planner/operator/logical_plan.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

bool PreparedStatement::isTransactionStatement() const {
    return preparedSummary.statementType == StatementType::TRANSACTION;
}

bool PreparedStatement::isSuccess() const {
    return success;
}

std::string PreparedStatement::getErrorMessage() const {
    return errMsg;
}

bool PreparedStatement::isReadOnly() const {
    return readOnly;
}

bool PreparedStatement::isProfile() {
    return logicalPlans[0]->isProfile();
}

StatementType PreparedStatement::getStatementType() {
    return parsedStatement->getStatementType();
}

PreparedStatement::~PreparedStatement() = default;

} // namespace main
} // namespace kuzu
