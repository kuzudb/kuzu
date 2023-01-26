#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

struct internalID_t;
typedef internalID_t nodeID_t;
typedef internalID_t relID_t;

typedef uint64_t table_id_t;
typedef uint64_t offset_t;
constexpr table_id_t INVALID_TABLE_ID = UINT64_MAX;
constexpr offset_t INVALID_NODE_OFFSET = UINT64_MAX;

// System representation for internalID.
struct internalID_t {
    offset_t offset;
    table_id_t tableID;

    internalID_t() = default;
    internalID_t(offset_t offset, table_id_t tableID) : offset(offset), tableID(tableID) {}

    // comparison operators
    inline bool operator==(const internalID_t& rhs) const {
        return offset == rhs.offset && tableID == rhs.tableID;
    };
    inline bool operator!=(const internalID_t& rhs) const {
        return offset != rhs.offset || tableID != rhs.tableID;
    };
    inline bool operator>(const internalID_t& rhs) const {
        return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset > rhs.offset);
    };
    inline bool operator>=(const internalID_t& rhs) const {
        return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset >= rhs.offset);
    };
    inline bool operator<(const internalID_t& rhs) const {
        return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset < rhs.offset);
    };
    inline bool operator<=(const internalID_t& rhs) const {
        return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset <= rhs.offset);
    };
};

} // namespace common
} // namespace kuzu
