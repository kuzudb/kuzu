#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/api.h"

namespace kuzu {
namespace common {

// table id type alias
using table_id_t = uint64_t;
using table_id_vector_t = std::vector<table_id_t>;
using table_id_set_t = std::unordered_set<table_id_t>;
template<typename T>
using table_id_map_t = std::unordered_map<table_id_t, T>;
constexpr table_id_t INVALID_TABLE_ID = UINT64_MAX;

// offset type alias
using offset_t = uint64_t;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;

// internal id type alias
struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

// System representation for internalID.
struct KUZU_API internalID_t {
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
