#pragma once

#include <string>

#include "common/exception/binder.h"
#include "common/types/types.h"

namespace kuzu {
namespace function {

// The maximum number of iterations to run.
struct MaxIterations {
    static constexpr const char* NAME = "maxiterations";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;

    static void validate(int64_t maxIterations) {
        if (maxIterations < 0) {
            throw common::BinderException{"Max iteration must be a positive integer."};
        }
    }
};

} // namespace function
} // namespace kuzu
