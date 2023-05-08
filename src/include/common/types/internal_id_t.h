#pragma once

#include <cstdint>

#include "common/api.h"

namespace kuzu {
namespace common {

struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

using table_id_t = uint64_t;
using offset_t = uint64_t;
constexpr table_id_t INVALID_TABLE_ID = UINT64_MAX;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;

// System representation for internalID.
KUZU_API struct internalID_t {
    offset_t offset;
    table_id_t tableID;

    internalID_t();
    internalID_t(offset_t offset, table_id_t tableID);

    // comparison operators
    bool operator==(const internalID_t& rhs) const;
    bool operator!=(const internalID_t& rhs) const;
    bool operator>(const internalID_t& rhs) const;
    bool operator>=(const internalID_t& rhs) const;
    bool operator<(const internalID_t& rhs) const;
    bool operator<=(const internalID_t& rhs) const;
};

} // namespace common
} // namespace kuzu
