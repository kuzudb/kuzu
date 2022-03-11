#pragma once

namespace graphflow {
namespace common {

typedef uint64_t label_t;
typedef uint64_t node_offset_t;

// System representation for nodeID.
struct nodeID_t {
    node_offset_t offset;
    label_t label;

    nodeID_t() = default;
    explicit inline nodeID_t(node_offset_t _offset, label_t _label)
        : offset(_offset), label(_label) {}

    // comparison operators
    inline bool operator==(const nodeID_t& rhs) const {
        return offset == rhs.offset && label == rhs.label;
    };
    inline bool operator!=(const nodeID_t& rhs) const {
        return offset != rhs.offset || label != rhs.label;
    };
    inline bool operator>(const nodeID_t& rhs) const {
        return (label > rhs.label) || (label == rhs.label && offset > rhs.offset);
    };
    inline bool operator>=(const nodeID_t& rhs) const {
        return (label > rhs.label) || (label == rhs.label && offset >= rhs.offset);
    };
    inline bool operator<(const nodeID_t& rhs) const {
        return (label < rhs.label) || (label == rhs.label && offset < rhs.offset);
    };
    inline bool operator<=(const nodeID_t& rhs) const {
        return (label < rhs.label) || (label == rhs.label && offset <= rhs.offset);
    };
};

} // namespace common
} // namespace graphflow
