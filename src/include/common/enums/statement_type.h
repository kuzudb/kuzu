#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_TABLE = 1,
    DROP_TABLE = 2,
    ALTER = 3,
    COPY_TO = 19,
    COPY_FROM = 20,
    STANDALONE_CALL = 21,
    EXPLAIN = 22,
    CREATE_MACRO = 23,
    TRANSACTION = 30,
    EXTENSION = 31,
    EXPORT_DATABASE = 32,
    IMPORT_DATABASE = 33,
    ATTACH_DATABASE = 34,
    DETACH_DATABASE = 35,
    USE_DATABASE = 36,
    CREATE_SEQUENCE = 37,
    DROP_SEQUENCE = 38,
    CREATE_TYPE = 39,
};

} // namespace common
} // namespace kuzu
