#pragma once

#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

struct Direction {
    static constexpr const char* NAME = "direction";
    static constexpr const char* UNDIRECTED = "undirected";
    static constexpr const char* DIRECTED = "directed";
    static constexpr const char* DEFAULT_VALUE = DIRECTED;
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static void validate(std::string dir) {
        if (dir != DIRECTED && dir != UNDIRECTED) {
            throw common::BinderException(common::stringFormat(
                "{} argument expects '{}' or '{}'. Got: {}", NAME, UNDIRECTED, DIRECTED, dir));
        }
    }
};

struct WeightProperty {
    static constexpr const char* NAME = "weight_property";
    static constexpr const char* DEFAULT_VALUE = "";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
};

struct SFConfig final : public GDSConfig {
    std::string direction = Direction::DEFAULT_VALUE;
    std::string weightProperty;
    SFConfig() = default;
};

} // namespace function
} // namespace kuzu
