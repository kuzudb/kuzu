#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

// TODO(Guodong/Ziyi/Xiyang): Should we remove this and instead use `CatalogEntryType`?
enum class TableType : uint8_t {
    UNKNOWN = 0,
    NODE = 1,
    REL = 2,
    REL_GROUP = 4,
    FOREIGN = 5,
};

struct TableTypeUtils {
    static std::string toString(TableType tableType);
};

} // namespace common
} // namespace kuzu
