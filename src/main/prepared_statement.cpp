#include "main/prepared_statement.h"

#include "binder/expression/expression.h" // IWYU pragma: keep
#include "common/exception/binder.h"
#include "common/types/value/value.h"
#include "planner/operator/logical_plan.h" // IWYU pragma: keep

using namespace kuzu::common;

namespace kuzu {
namespace main {

CachedPreparedStatement::CachedPreparedStatement() = default;
CachedPreparedStatement::~CachedPreparedStatement() = default;

std::vector<std::string> CachedPreparedStatement::getColumnNames() const {
    std::vector<std::string> names;
    for (auto& column : columns) {
        names.push_back(column->toString());
    }
    return names;
}

std::vector<LogicalType> CachedPreparedStatement::getColumnTypes() const {
    std::vector<LogicalType> types;
    for (auto& column : columns) {
        types.push_back(column->getDataType().copy());
    }
    return types;
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

StatementType PreparedStatement::getStatementType() const {
    return preparedSummary.statementType;
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

std::unique_ptr<PreparedStatement> PreparedStatement::getPreparedStatementWithError(
    const std::string& errorMessage) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    preparedStatement->success = false;
    preparedStatement->errMsg = errorMessage;
    return preparedStatement;
}

} // namespace main
} // namespace kuzu
