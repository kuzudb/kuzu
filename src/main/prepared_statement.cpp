#include "main/prepared_statement.h"

#include "binder/bound_statement_result.h"
#include "common/statement_type.h"
#include "planner/logical_plan/logical_plan.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

bool PreparedStatement::allowActiveTransaction() const {
    return !StatementTypeUtils::allowActiveTransaction(preparedSummary.statementType);
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

PreparedStatement::~PreparedStatement() = default;

} // namespace main
} // namespace kuzu
