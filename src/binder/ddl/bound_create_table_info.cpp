#include "binder/ddl/bound_create_table_info.h"

namespace kuzu {
namespace binder {

std::vector<std::unique_ptr<BoundCreateTableInfo>> BoundCreateTableInfo::copy(
    const std::vector<std::unique_ptr<BoundCreateTableInfo>>& infos) {
    std::vector<std::unique_ptr<BoundCreateTableInfo>> result;
    result.reserve(infos.size());
    for (auto& info : infos) {
        result.push_back(info->copy());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
