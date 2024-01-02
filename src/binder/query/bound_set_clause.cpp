#include "binder/query/updating_clause/bound_set_clause.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

bool BoundSetClause::hasInfo(const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    for (auto& info : infos) {
        if (check(info)) {
            return true;
        }
    }
    return false;
}

std::vector<const BoundSetPropertyInfo*> BoundSetClause::getInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
    std::vector<const BoundSetPropertyInfo*> result;
    for (auto& info : infos) {
        if (check(info)) {
            result.push_back(&info);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
