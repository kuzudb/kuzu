#include "main/prepared_statement.h"

#include "binder/bound_statement_result.h" // IWYU pragma: keep (used to avoid error in destructor)
#include "common/enums/statement_type.h"
#include "common/exception/binder.h"
#include "common/types/value/value.h"
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

bool PreparedStatement::isProfile() const {
    return logicalPlan->isProfile();
}

StatementType PreparedStatement::getStatementType() const {
    return parsedStatement->getStatementType();
}

void PreparedStatement::validateExecuteParam(const std::string& paramName,
    common::Value* param) const {
    if (param->getDataType().getLogicalTypeID() == LogicalTypeID::POINTER &&
        (!parameterMap.contains(paramName) ||
            parameterMap.at(paramName)->getValue<uint8_t*>() != param->getValue<uint8_t*>())) {
        throw BinderException(stringFormat(
            "When preparing the current statement the dataframe passed into parameter "
            "'{}' was different from the one provided during prepare. Dataframes parameters "
            "are only used during prepare; please make sure that they are either not passed into "
            "execute or they match the one passed during prepare.",
            paramName));
    }
}

PreparedStatement::~PreparedStatement() = default;

} // namespace main
} // namespace kuzu
