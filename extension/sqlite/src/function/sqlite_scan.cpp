#include "function/sqlite_scan.h"

namespace kuzu {
namespace sqlite_extension {

std::string SqliteScanBindData::getQuery(const main::ClientContext& context) const {
    auto setStringQueryResult = common::stringFormat("set sqlite_all_varchar={};",
        context.getCurrentSetting("sqlite_all_varchar").getValue<bool>());
    return setStringQueryResult + query;
}

std::vector<common::LogicalType> SqliteScanBindData::getColumnTypes(
    const main::ClientContext& context) const {
    auto result = copyVector(columnTypes);
    if (context.getCurrentSetting("sqlite_all_varchar").getValue<bool>()) {
        for (auto i = 0u; i < columnTypes.size(); i++) {
            result[i] = common::LogicalType::STRING();
        }
    }
    return result;
}

} // namespace sqlite_extension
} // namespace kuzu
