#pragma once

#include <vector>

#include "common/types/value/value.h"

namespace kuzu {
namespace function {

struct TableFuncBindInput {
    explicit TableFuncBindInput(std::vector<common::Value> inputs) : inputs{std::move(inputs)} {}
    std::vector<common::Value> inputs;
};

} // namespace function
} // namespace kuzu
