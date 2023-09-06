#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_TABLE = 1,
    DROP_TABLE = 4,
    RENAME_TABLE = 5,
    ADD_PROPERTY = 6,
    DROP_PROPERTY = 7,
    RENAME_PROPERTY = 8,
    COPY_TO = 19,
    COPY_FROM = 20,
    STANDALONE_CALL = 21,
    EXPLAIN = 22,
    CREATE_MACRO = 23,
    TRANSACTION = 30,
};

struct StatementTypeUtils {
    static bool allowActiveTransaction(StatementType statementType) {
        switch (statementType) {
        case StatementType::CREATE_TABLE:
        case StatementType::DROP_TABLE:
        case StatementType::RENAME_TABLE:
        case StatementType::ADD_PROPERTY:
        case StatementType::DROP_PROPERTY:
        case StatementType::RENAME_PROPERTY:
        case StatementType::CREATE_MACRO:
        case StatementType::COPY_FROM:
            return true;
        default:
            return false;
        }
    }
};

} // namespace common
} // namespace kuzu
