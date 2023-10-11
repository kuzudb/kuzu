#include "binder/query/updating_clause/bound_merge_clause.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

BoundMergeClause::BoundMergeClause(const BoundMergeClause& other)
    : BoundUpdatingClause{ClauseType::MERGE} {
    queryGraphCollection = other.queryGraphCollection->copy();
    predicate = other.predicate;
    for (auto& info : other.insertInfos) {
        insertInfos.push_back(info->copy());
    }
    for (auto& info : other.onMatchSetPropertyInfos) {
        onMatchSetPropertyInfos.push_back(info->copy());
    }
    for (auto& info : other.onCreateSetPropertyInfos) {
        onCreateSetPropertyInfos.push_back(info->copy());
    }
}

bool BoundMergeClause::hasInsertInfo(
    const std::function<bool(const BoundInsertInfo&)>& check) const {
    for (auto& info : insertInfos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundInsertInfo*> BoundMergeClause::getInsertInfos(
    const std::function<bool(const BoundInsertInfo&)>& check) const {
    std::vector<BoundInsertInfo*> result;
    for (auto& info : insertInfos) {
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
