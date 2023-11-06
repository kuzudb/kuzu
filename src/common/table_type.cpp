#include "common/enums/table_type.h"

#include "common/exception/not_implemented.h"

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
    default:                                                       // LCOV_EXCL_START
        throw NotImplementedException("TableTypeUtils::toString"); // LCOV_EXCL_STOP
    }
}

} // namespace common
} // namespace kuzu
