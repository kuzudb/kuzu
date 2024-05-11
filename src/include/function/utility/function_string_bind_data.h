#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct FunctionStringBindData : public FunctionBindData {
    explicit FunctionStringBindData(std::string str)
        : FunctionBindData{common::LogicalType::STRING()}, str{std::move(str)} {}

    std::string str;
};

} // namespace function
} // namespace kuzu
