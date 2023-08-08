#include "binder/query/updating_clause/bound_merge_clause.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

BoundMergeClause::BoundMergeClause(const BoundMergeClause& other)
    : BoundUpdatingClause{ClauseType::MERGE} {
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

bool BoundMergeClause::hasOnMatchSetInfo(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    for (auto& info : onMatchSetPropertyInfos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundSetPropertyInfo*> BoundMergeClause::getOnMatchSetInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    std::vector<BoundSetPropertyInfo*> result;
    for (auto& info : onMatchSetPropertyInfos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

bool BoundMergeClause::hasOnCreateSetInfo(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    for (auto& info : onCreateSetPropertyInfos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundSetPropertyInfo*> BoundMergeClause::getOnCreateSetInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    std::vector<BoundSetPropertyInfo*> result;
    for (auto& info : onCreateSetPropertyInfos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
