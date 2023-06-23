#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_NODE_TABLE = 1,
    CREATE_REL_TABLE = 2,
    DROP_TABLE = 4,
    RENAME_TABLE = 5,
    ADD_PROPERTY = 6,
    DROP_PROPERTY = 7,
    RENAME_PROPERTY = 8,
    COPY = 20,
    CALL = 21,
};

class StatementTypeUtils {
public:
    static bool isDDL(StatementType statementType) {
        return statementType == StatementType::CREATE_NODE_TABLE ||
               statementType == StatementType::CREATE_REL_TABLE ||
               statementType == StatementType::DROP_TABLE ||
               statementType == StatementType::DROP_PROPERTY;
    }

    static bool isCopyCSV(StatementType statementType) {
        return statementType == StatementType::COPY;
    }

    static bool isDDLOrCopyCSV(StatementType statementType) {
        return isDDL(statementType) || isCopyCSV(statementType);
    }
};

} // namespace common
} // namespace kuzu
