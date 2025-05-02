#pragma once

#include <string>

#include "common/exception/binder.h"
#include "common/types/types.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

// The maximum number of phases in which the graph is clustered and then aggregated.
struct MaxPhases {
    static constexpr const char* NAME = "maxphases";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 20;

    static void validate(int64_t maxPhases) {
        if (maxPhases < 0) {
            throw common::BinderException{"maxphases must be a positive integer."};
        }
    }
};

struct LouvainConfig final : public GDSConfig {
    uint64_t maxIterations = 20;
    uint64_t maxPhases = MaxPhases::DEFAULT_VALUE;

    LouvainConfig() = default;
};

} // namespace function
} // namespace kuzu
