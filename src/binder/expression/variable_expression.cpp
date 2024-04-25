#include "binder/expression/variable_expression.h"

#include "common/exception/binder.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void VariableExpression::cast(const LogicalType& type) {
    if (!dataType.containsAny()) {
        // LCOV_EXCL_START
        throw BinderException(
            stringFormat("Cannot change variable expression data type from {} to {}.",
                dataType.toString(), type.toString()));
        // LCOV_EXCL_STOP
    }
    dataType = type;
}

} // namespace binder
} // namespace kuzu
