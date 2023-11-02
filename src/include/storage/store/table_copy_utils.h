#pragma once

#include <cstdint>

namespace kuzu {
namespace storage {

// TODO(Guodong): Move this to StorageUtils.
class TableCopyUtils {
public:
    static void validateStrLen(uint64_t strLen);
};

} // namespace storage
} // namespace kuzu
