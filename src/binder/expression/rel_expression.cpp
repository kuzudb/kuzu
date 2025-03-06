#include "binder/expression/rel_expression.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction_util.h"
#include "common/exception/binder.h"
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
    } break;
    case QueryRelType::ALL_WEIGHTED_SHORTEST: {
        result += "ALL WEIGHTED SHORTEST";
    } break;
    default:
        break;
    }
    if (QueryRelTypeUtils::isRecursive(relType)) {
        result += std::to_string(recursiveInfo->bindData->lowerBound);
        result += "..";
        result += std::to_string(recursiveInfo->bindData->upperBound);
    }
    return result;
}

std::vector<common::ExtendDirection> RelExpression::getExtendDirections() const {
    std::vector<ExtendDirection> ret;
    for (const auto direction : {ExtendDirection::FWD, ExtendDirection::BWD}) {
        const bool addDirection = std::all_of(entries.begin(), entries.end(),
            [direction](const catalog::TableCatalogEntry* tableEntry) {
                const auto* relTableEntry =
                    tableEntry->constPtrCast<catalog::RelTableCatalogEntry>();
                return common::containsValue(relTableEntry->getRelDataDirections(),
                    ExtendDirectionUtil::getRelDataDirection(direction));
            });
        if (addDirection) {
            ret.push_back(direction);
        }
    }
    if (ret.empty()) {
        throw BinderException(stringFormat(
            "There are no common storage directions among the rel "
            "tables matched by pattern '{}' (some tables have storage direction 'fwd' "
            "while others have storage direction 'bwd'). Scanning different tables matching the "
            "same pattern in different directions is currently unsupported.",
            toString()));
    }
    return ret;
}

} // namespace binder
} // namespace kuzu
