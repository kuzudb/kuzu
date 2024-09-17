#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace storage {

enum class DBFileType : uint8_t {
    NODE_INDEX = 0,
    DATA = 1,
};

// DBFileID start with 1 byte type  followed with additional bytes needed by node hash index
// (isOverflow and tableID).
struct DBFileID {
    DBFileType dbFileType = DBFileType::DATA;
    bool isOverflow = false;
    common::table_id_t tableID = common::INVALID_TABLE_ID;

    DBFileID() = default;
    bool operator==(const DBFileID& rhs) const = default;

    static DBFileID newDataFileID();
    static DBFileID newPKIndexFileID(common::table_id_t tableID);
};

} // namespace storage
} // namespace kuzu
