#include "common/enums/table_type.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

std::string TableTypeUtils::toString(TableType tableType) {
    switch (tableType) {
    case TableType::UNKNOWN: {
        return "UNKNOWN";
    }
    case TableType::NODE: {
        return "NODE";
    }
    case TableType::REL: {
        return "REL";
    }
    case TableType::RDF: {
        return "RDF";
    }
    case TableType::REL_GROUP: {
        return "REL_GROUP";
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
