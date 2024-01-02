#include "binder/ddl/bound_create_table_info.h"

namespace kuzu {
namespace binder {

std::vector<BoundCreateTableInfo> BoundCreateTableInfo::copy(
    const std::vector<BoundCreateTableInfo>& infos) {
    std::vector<BoundCreateTableInfo> result;
    result.reserve(infos.size());
    for (auto& info : infos) {
        result.push_back(info.copy());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
