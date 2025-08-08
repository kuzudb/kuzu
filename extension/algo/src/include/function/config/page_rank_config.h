#pragma once

#include <string>

#include "common/exception/binder.h"
#include "common/types/types.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

struct DampingFactor {
    // The damping factor of the Page Rank calculation. Must be in [0, 1).
    static constexpr const char* NAME = "dampingfactor";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.85;

    static void validate(double dampingFactor) {
        if (dampingFactor < 0 || dampingFactor >= 1) {
            throw common::BinderException{"Damping factor must be in the [0, 1)."};
        }
    }
};

struct Tolerance {
    // Minimum change in scores between iterations. If all scores change less than the tolerance
    // value the result is considered stable and the algorithm returns.
    static constexpr const char* NAME = "tolerance";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.0000001;
};

struct NormalizeInitial {
    // If true we initialize rank to 1/num_nodes. Otherwise, initialize to 1.
    static constexpr const char* NAME = "normalizeinitial";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::BOOL;
    static constexpr bool DEFAULT_VALUE = true;
};

} // namespace function
} // namespace kuzu
