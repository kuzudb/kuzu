#include "binder/query/updating_clause/bound_merge_clause.h"

namespace kuzu {
namespace binder {

BoundMergeClause::BoundMergeClause(const BoundMergeClause& other)
    : BoundUpdatingClause{common::ClauseType::MERGE} {
    queryGraphCollection = other.queryGraphCollection->copy();
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

} // namespace binder
} // namespace kuzu
