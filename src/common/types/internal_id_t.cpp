#include "common/types/internal_id_t.h"

namespace kuzu {
namespace common {

internalID_t::internalID_t() : offset{INVALID_OFFSET}, tableID{INVALID_TABLE_ID} {}

internalID_t::internalID_t(offset_t offset, table_id_t tableID)
    : offset(offset), tableID(tableID) {}

bool internalID_t::operator==(const internalID_t& rhs) const {
    return offset == rhs.offset && tableID == rhs.tableID;
}

bool internalID_t::operator!=(const internalID_t& rhs) const {
    return offset != rhs.offset || tableID != rhs.tableID;
}

bool internalID_t::operator>(const internalID_t& rhs) const {
    return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset > rhs.offset);
}

bool internalID_t::operator>=(const internalID_t& rhs) const {
    return (tableID > rhs.tableID) || (tableID == rhs.tableID && offset >= rhs.offset);
}

bool internalID_t::operator<(const internalID_t& rhs) const {
    return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset < rhs.offset);
}

bool internalID_t::operator<=(const internalID_t& rhs) const {
    return (tableID < rhs.tableID) || (tableID == rhs.tableID && offset <= rhs.offset);
}

} // namespace common
} // namespace kuzu