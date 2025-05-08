#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct ListFunctionUtils {
public:
    static void resolveEmptyList(const ScalarBindFuncInput& input,
        std::vector<common::LogicalType>& types);

    using empty_type_resolver = std::function<void(std::vector<common::LogicalType>& types,
        common::LogicalType& targetType)>;

    static void resolveNulls(std::vector<common::LogicalType>& types, empty_type_resolver bothNull,
        empty_type_resolver leftNull, empty_type_resolver rightNull,
        empty_type_resolver finalTypeResolver);

    static void checkTypes(const ScalarBindFuncInput& input,
        std::vector<common::LogicalType>& types, const char* name);
};

} // namespace function
} // namespace kuzu
