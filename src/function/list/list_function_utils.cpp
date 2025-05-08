#include "function/list/functions/list_function_utils.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void ListFunctionUtils::resolveEmptyList(const ScalarBindFuncInput& input,
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

void ListFunctionUtils::resolveNulls(std::vector<common::LogicalType>& types,
    empty_type_resolver bothNull, empty_type_resolver leftNull, empty_type_resolver rightNull,
    empty_type_resolver finalTypeResolver) {
    auto isArg0AnyType = types[0].getLogicalTypeID() == common::LogicalTypeID::ANY;
    auto isArg1AnyType = types[1].getLogicalTypeID() == common::LogicalTypeID::ANY;

    common::LogicalType targetType;
    if (isArg0AnyType && isArg1AnyType) {
        bothNull(types, targetType);
    } else if (isArg0AnyType) {
        leftNull(types, targetType);
    } else if (isArg1AnyType) {
        rightNull(types, targetType);
    } else {
        return;
    }
    finalTypeResolver(types, targetType);
}

void ListFunctionUtils::checkTypes(const ScalarBindFuncInput& input,
    std::vector<common::LogicalType>& types, const char* name) {
    resolveEmptyList(input, types);
    empty_type_resolver bothNullResolver = [](std::vector<common::LogicalType>& types,
                                               common::LogicalType& targetType) {
        targetType = common::LogicalType::INT64();
    };
    empty_type_resolver leftNullResolver = [](std::vector<common::LogicalType>& types,
                                               common::LogicalType& targetType) {
        targetType = types[0].copy();
    };
    empty_type_resolver rightNullResolver = [](std::vector<common::LogicalType>& types,
                                                common::LogicalType& targetType) {
        targetType = common::ListType::getChildType(types[1]).copy();
    };
    empty_type_resolver finalTypeResolver = [](std::vector<common::LogicalType>& types,
                                                common::LogicalType& targetType) {
        types[0] = common::LogicalType::LIST(targetType.copy());
        types[1] = targetType.copy();
    };
    resolveNulls(types, bothNullResolver, leftNullResolver, rightNullResolver, finalTypeResolver);

    if (types[0].getLogicalTypeID() != LogicalTypeID::ANY &&
        types[1] != ListType::getChildType(types[0]))
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(name,
            types[0].toString(), types[1].toString()));
}

} // namespace function
} // namespace kuzu
