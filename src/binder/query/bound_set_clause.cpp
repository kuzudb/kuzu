#include "binder/query/updating_clause/bound_set_clause.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

BoundSetClause::BoundSetClause(const BoundSetClause& other) : BoundUpdatingClause{ClauseType::SET} {
    for (auto& info : other.infos) {
        infos.push_back(info->copy());
    }
}

bool BoundSetClause::hasInfo(const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    for (auto& info : infos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundSetPropertyInfo*> BoundSetClause::getInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    std::vector<BoundSetPropertyInfo*> result;
    for (auto& info : infos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
