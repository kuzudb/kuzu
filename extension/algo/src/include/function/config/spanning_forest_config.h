#pragma once

#include <string>

#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

struct Variant {
    static constexpr const char* NAME = "variant";
    static constexpr const char* MAX_VARIANT = "max";
    static constexpr const char* MIN_VARIANT = "min";
    static constexpr const char* DEFAULT_VALUE = MIN_VARIANT;
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static void validate(std::string variant) {
        if (variant != MAX_VARIANT && variant != MIN_VARIANT) {
            throw common::BinderException(common::stringFormat(
                "Variant argument expects {} or {}. Got: {}", MAX_VARIANT, MIN_VARIANT, variant));
        }
    }
};

struct WeightProperty {
    static constexpr const char* NAME = "weight_property";
    static constexpr const char* DEFAULT_VALUE = "";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
};

struct SFConfig final : public GDSConfig {
    std::string variant = Variant::DEFAULT_VALUE;
    std::string weightProperty;
    SFConfig() = default;
};

} // namespace function
} // namespace kuzu
