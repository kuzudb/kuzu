#pragma once

#include <vector>

#include "common/types/value/value.h"

namespace kuzu {
namespace function {

struct TableFuncBindInput {
    explicit TableFuncBindInput(std::vector<std::unique_ptr<common::Value>> inputs)
        : inputs{std::move(inputs)} {}
    std::vector<std::unique_ptr<common::Value>> inputs;
};

} // namespace function
} // namespace kuzu
