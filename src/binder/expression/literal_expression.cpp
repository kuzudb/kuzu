#include "binder/expression/literal_expression.h"

#include "common/exception/binder.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void LiteralExpression::cast(const LogicalType& type) {
    // The following is a safeguard to make sure we are not changing literal type unexpectedly.
    if (!value.allowTypeChange()) {
        // LCOV_EXCL_START
        throw BinderException(
            stringFormat("Cannot change literal expression data type from {} to {}.",
                dataType.toString(), type.toString()));
        // LCOV_EXCL_STOP
    }
    dataType = type.copy();
    value.setDataType(type);
}

} // namespace binder
} // namespace kuzu
