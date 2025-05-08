#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct ListFunctionUtils {
public:
    static inline void resolveEmptyList(const ScalarBindFuncInput& input,
        std::vector<common::LogicalType>& types); 

    static inline void resolveNulls(std::vector<common::LogicalType>& types); 
};

} // namespace function
} // namespace kuzu
