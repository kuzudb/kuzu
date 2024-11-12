#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace json_extension {

struct JsonType {
    static common::LogicalType getJsonType();
};

} // namespace json_extension
} // namespace kuzu
