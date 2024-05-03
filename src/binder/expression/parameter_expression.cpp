#include "binder/expression/parameter_expression.h"

#include "common/exception/binder.h"

namespace kuzu {
using namespace common;

namespace binder {

void ParameterExpression::cast(const LogicalType& type) {
    if (!dataType.containsAny()) {
        // LCOV_EXCL_START
        throw BinderException(
            stringFormat("Cannot change parameter expression data type from {} to {}.",
                dataType.toString(), type.toString()));
        // LCOV_EXCL_STOP
    }
    dataType = type;
    value.setDataType(type);
}

} // namespace binder
} // namespace kuzu
