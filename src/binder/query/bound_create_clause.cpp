#include "binder/query/updating_clause/bound_create_clause.h"

namespace kuzu {
namespace binder {

BoundCreateClause::BoundCreateClause(const BoundCreateClause& other)
    : BoundUpdatingClause{common::ClauseType::CREATE} {
    for (auto& info : other.infos) {
        infos.push_back(info->copy());
    }
}

std::vector<expression_pair> BoundCreateClause::getAllSetItems() const {
    std::vector<expression_pair> result;
    for (auto& info : infos) {
        for (auto& setItem : info->setItems) {
            result.push_back(setItem);
        }
    }
    return result;
}

bool BoundCreateClause::hasInfo(const std::function<bool(const BoundCreateInfo&)>& check) const {
    for (auto& info : infos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundCreateInfo*> BoundCreateClause::getInfos(
    const std::function<bool(const BoundCreateInfo&)>& check) const {
    std::vector<BoundCreateInfo*> result;
    for (auto& info : infos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
