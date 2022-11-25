#pragma once

namespace kuzu {
namespace common {

typedef uint64_t table_id_t;
typedef uint64_t node_offset_t;
constexpr node_offset_t INVALID_NODE_OFFSET = UINT64_MAX;

// System representation for nodeID.
struct nodeID_t {
    node_offset_t offset;
    table_id_t tableID;

    nodeID_t() = default;
    explicit inline nodeID_t(node_offset_t _offset, table_id_t tableID)
        : offset(_offset), tableID(tableID) {}

    // comparison operators
    inline bool operator==(const nodeID_t& rhs) const {
        return offset == rhs.offset && tableID == rhs.tableID;
    };
    inline bool operator!=(const nodeID_t& rhs) const {
        return offset != rhs.offset || tableID != rhs.tableID;
    };
    inline bool operator>(const nodeID_t& rhs) const {
        return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset > rhs.offset);
    };
    inline bool operator>=(const nodeID_t& rhs) const {
        return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset >= rhs.offset);
    };
    inline bool operator<(const nodeID_t& rhs) const {
        return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset < rhs.offset);
    };
    inline bool operator<=(const nodeID_t& rhs) const {
        return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset <= rhs.offset);
    };
};

} // namespace common
} // namespace kuzu
