#pragma once

#include <cstdint>

namespace kuzu {
namespace json_extension {

enum class JSONRecordType : uint8_t {
    AUTO_DETECT = 0,
    RECORDS = 1,
    VALUES = 2,
};

enum class JsonScanFormat : uint8_t {
    AUTO_DETECT = 0,
    // One unit after another, newlines can be anywhere
    UNSTRUCTURED = 1,
    // Units are separated by newlines, newlines do not occur within units (NDJSON).
    NEWLINE_DELIMITED = 2,
    // File is one big array of units
    ARRAY = 3,
};

} // namespace json_extension
} // namespace kuzu
