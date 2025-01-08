#include "binder/expression/rel_expression.h"

#include <numeric>

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

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

std::vector<common::ExtendDirection> RelExpression::getExtendDirections() const {
    return std::accumulate(entries.begin(), entries.end(),
        std::vector{ExtendDirection::FWD, ExtendDirection::BWD},
        [](auto intersection, catalog::TableCatalogEntry* curEntry) {
            decltype(intersection) ret;
            const auto* relTableEntry = curEntry->constPtrCast<catalog::RelTableCatalogEntry>();
            const auto newDirections = relTableEntry->getExtendDirections();
            std::set_intersection(intersection.begin(), intersection.end(), newDirections.begin(),
                newDirections.end(), std::back_inserter(ret));
            return ret;
        });
}

} // namespace binder
} // namespace kuzu
