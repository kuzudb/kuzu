#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct ListFunctionUtils {
public:
    static void resolveEmptyList(const ScalarBindFuncInput& input,
        std::vector<common::LogicalType>& types);

    static void resolveNulls(std::vector<common::LogicalType>& types);
    static void checkTypes(const ScalarBindFuncInput& input,
        std::vector<common::LogicalType>& types, const char* name);

};

} // namespace function
} // namespace kuzu
