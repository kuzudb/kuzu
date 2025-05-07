#pragma once

#include "binder/expression/expression_util.h"
#include "function/function.h"

namespace kuzu {
namespace function {

static inline void resolveEmptyList(const ScalarBindFuncInput& input,
    std::vector<common::LogicalType>& types) {
    auto isEmpty = binder::ExpressionUtil::isEmptyList(*input.arguments[0]);
    if (isEmpty) {
        auto elementType = input.arguments[1]->getDataType().copy();
        if (elementType.getLogicalTypeID() == common::LogicalTypeID::ANY)
            elementType = common::LogicalType(common::LogicalTypeID::INT64);

        types[0] = common::LogicalType::LIST(elementType.copy()).copy();
        types[1] = elementType.copy();
    }
}

static inline void resolveNulls(std::vector<common::LogicalType>& types) {
    auto isArg0AnyType = types[0].getLogicalTypeID() == common::LogicalTypeID::ANY;
    auto isArg1AnyType = types[1].getLogicalTypeID() == common::LogicalTypeID::ANY;

    common::LogicalType targetType;
    if (isArg0AnyType && isArg1AnyType) {
        targetType = common::LogicalType::INT64();
    } else if (isArg0AnyType) {
        targetType = types[0].copy();
    } else if (isArg1AnyType) {
        targetType = common::ListType::getChildType(types[1]).copy();
    } else {
        return;
    }
    types[0] = common::LogicalType::LIST(targetType.copy());
    types[1] = targetType.copy();
}

} // namespace function
} // namespace kuzu
