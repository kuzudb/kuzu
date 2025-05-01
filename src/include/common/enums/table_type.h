#pragma once

#include <cstdint>
#include <string>

#include "common/api.h"

namespace kuzu {
namespace common {

// TODO(Guodong/Ziyi/Xiyang): Should we remove this and instead use `CatalogEntryType`?
enum class TableType : uint8_t {
    UNKNOWN = 0,
    NODE = 1,
    REL = 2,
    FOREIGN = 5,
};

struct KUZU_API TableTypeUtils {
    static std::string toString(TableType tableType);
};

} // namespace common
} // namespace kuzu
