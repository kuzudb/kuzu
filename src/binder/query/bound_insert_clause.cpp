#include "binder/query/updating_clause/bound_insert_clause.h"

#include <functional>
#include <vector>

#include "binder/query/updating_clause/bound_insert_info.h"
#include "binder/query/updating_clause/bound_updating_clause.h"
#include "common/enums/clause_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

BoundInsertClause::BoundInsertClause(const BoundInsertClause& other)
    : BoundUpdatingClause{ClauseType::INSERT} {
    for (auto& info : other.infos) {
        infos.push_back(info->copy());
    }
}

bool BoundInsertClause::hasInfo(const std::function<bool(const BoundInsertInfo&)>& check) const {
    for (auto& info : infos) {
        if (check(*info)) {
            return true;
        }
    }
    return false;
}

std::vector<BoundInsertInfo*> BoundInsertClause::getInfos(
    const std::function<bool(const BoundInsertInfo&)>& check) const {
    std::vector<BoundInsertInfo*> result;
    for (auto& info : infos) {
        if (check(*info)) {
            result.push_back(info.get());
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
