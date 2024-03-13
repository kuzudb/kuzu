#include "binder/expression/literal_expression.h"

#include "common/exception/binder.h"

namespace kuzu {
using namespace common;

namespace binder {

void LiteralExpression::cast(const LogicalType& type) {
    if (dataType.getLogicalTypeID() != LogicalTypeID::ANY) {
        // LCOV_EXCL_START
        throw BinderException(
            stringFormat("Cannot change literal expression data type from {} to {}.",
                dataType.toString(), type.toString()));
        // LCOV_EXCL_STOP
    }
    dataType = type;
    value->setDataType(type);
}

} // namespace binder
} // namespace kuzu
