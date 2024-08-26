#include "binder/expression/rel_expression.h"

#include "catalog/catalog_entry/table_catalog_entry.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

common::table_id_vector_t RdfPredicateInfo::getResourceTableIDs() const {
    common::table_id_vector_t tableIDs;
    for (auto entry : resourceEntries) {
        tableIDs.push_back(entry->getTableID());
    }
    return tableIDs;
}

std::string RelExpression::detailsToString() const {
    std::string result = toString();
    switch (relType) {
    case common::QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case common::QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    default:
        break;
    }
    if (common::QueryRelTypeUtils::isRecursive(relType)) {
        result += std::to_string(getLowerBound());
        result += "..";
        result += std::to_string(getUpperBound());
    }
    return result;
}

} // namespace binder
} // namespace kuzu