#include "main/prepared_statement.h"

#include "binder/bound_statement_result.h"
#include "common/statement_type.h"

namespace kuzu {
namespace main {

bool PreparedStatement::allowActiveTransaction() const {
    return !common::StatementTypeUtils::isDDLOrCopyCSV(statementType);
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

binder::expression_vector PreparedStatement::getExpressionsToCollect() {
    return statementResult->getExpressionsToCollect();
}

} // namespace main
} // namespace kuzu
