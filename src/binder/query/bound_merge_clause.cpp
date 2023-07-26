#include "binder/query/updating_clause/bound_merge_clause.h"

namespace kuzu {
namespace binder {

BoundMergeClause::BoundMergeClause(const BoundMergeClause& other)
    : BoundUpdatingClause{common::ClauseType::MERGE} {
    queryGraphCollection = other.queryGraphCollection->copy();
    predicate = other.predicate;
    for (auto& createInfo : other.createInfos) {
        createInfos.push_back(createInfo->copy());
    }
    for (auto& setPropertyInfo : other.onMatchSetPropertyInfos) {
        onMatchSetPropertyInfos.push_back(setPropertyInfo->copy());
    }
    for (auto& setPropertyInfo : other.onCreateSetPropertyInfos) {
        onCreateSetPropertyInfos.push_back(setPropertyInfo->copy());
    }
}

bool BoundMergeClause::hasCreateInfo(
    const std::function<bool(const BoundCreateInfo&)>& check) const {
    for (auto& info : createInfos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundCreateInfo*> BoundMergeClause::getCreateInfos(
    const std::function<bool(const BoundCreateInfo&)>& check) const {
    std::vector<BoundCreateInfo*> result;
    for (auto& info : createInfos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
