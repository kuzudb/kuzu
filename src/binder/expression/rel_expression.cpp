#include "binder/expression/rel_expression.h"

#include <numeric>

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::string RelExpression::detailsToString() const {
    std::string result = toString();
    switch (relType) {
    case QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    case QueryRelType::WEIGHTED_SHORTEST: {
        result += "WEIGHTED SHORTEST";
    }
    default:
        break;
    }
    if (QueryRelTypeUtils::isRecursive(relType)) {
        result += std::to_string(getLowerBound());
        result += "..";
        result += std::to_string(getUpperBound());
    }
    return result;
}

std::vector<common::ExtendDirection> RelExpression::getExtendDirections() const {
    return std::accumulate(entries.begin(), entries.end(),
        std::vector{ExtendDirection::FWD, ExtendDirection::BWD},
        [](const auto& intersection, catalog::TableCatalogEntry* curEntry) {
            const auto newDirections =
                curEntry->constPtrCast<catalog::RelTableCatalogEntry>()->getRelDataDirections();
            std::vector<ExtendDirection> ret;
            std::copy_if(intersection.begin(), intersection.end(), std::back_inserter(ret),
                [&](auto extendDir) {
                    return common::dataContains(newDirections,
                        ExtendDirectionUtil::getRelDataDirection(extendDir));
                });
            return ret;
        });
}

} // namespace binder
} // namespace kuzu
