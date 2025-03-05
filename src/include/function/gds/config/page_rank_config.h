#pragma once

#include <string>

#include "common/exception/binder.h"
#include "common/types/types.h"

namespace kuzu {
namespace function {

struct DampingFactor {
    // The damping factor of the Page Rank calculation. Must be in [0, 1).
    static constexpr const char* NAME = "dampingfactor";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.85;

    static void validate(double dampingFactor);
};

void DampingFactor::validate(double dampingFactor) {
    if (dampingFactor < 0 || dampingFactor >= 1) {
        throw common::BinderException{"Damping factor must be in the [0, 1)."};
    }
}

struct MaxIterations {
    // The maximum number of iterations of Page Rank to run.
    static constexpr const char* NAME = "maxiterations";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr uint64_t DEFAULT_VALUE = 20;

    static void validate(int64_t dampingFactor);
};

void MaxIterations::validate(int64_t maxIterations) {
    if (maxIterations < 0) {
        throw common::BinderException{"Max iteration must be a positive integer."};
    }
}

struct Tolerance {
    // Minimum change in scores between iterations. If all scores change less than the tolerance
    // value the result is considered stable and the algorithm returns.
    static constexpr const char* NAME = "tolerance";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.0000001;
};

struct PageRankConfig {
    double dampingFactor = DampingFactor::DEFAULT_VALUE;
    uint64_t maxIterations = MaxIterations::DEFAULT_VALUE;
    double tolerance = Tolerance::DEFAULT_VALUE;

    PageRankConfig() = default;
};

} // namespace function
} // namespace kuzu
