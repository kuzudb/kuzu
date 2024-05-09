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
    COMMENT_ON = 24,
    TRANSACTION = 30,
    EXTENSION = 31,
    EXPORT_DATABASE = 32,
    IMPORT_DATABASE = 33,
    ATTACH_DATABASE = 34,
    DETACH_DATABASE = 35,
    USE_DATABASE = 36,
};

struct StatementTypeUtils {
    static bool allowActiveTransaction(StatementType statementType) {
        switch (statementType) {
        case StatementType::COPY_FROM:
            return false;
        default:
            return true;
        }
    }
};

} // namespace common
} // namespace kuzu
