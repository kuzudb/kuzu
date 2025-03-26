#include "function/sqlite_scan.h"

#include "main/client_context.h"

namespace kuzu {
namespace sqlite_extension {

using namespace duckdb_extension;

std::string SQLiteTableScanInfo::getTemplateQuery(const main::ClientContext& context) const {
    auto setStringQueryResult = common::stringFormat("set sqlite_all_varchar={};",
        context.getCurrentSetting("sqlite_all_varchar").getValue<bool>());
    return setStringQueryResult + DuckDBTableScanInfo::getTemplateQuery(context);
}

std::vector<common::LogicalType> SQLiteTableScanInfo::getColumnTypes(
    const main::ClientContext& context) const {
    auto columnTypes = DuckDBTableScanInfo::getColumnTypes(context);
    if (context.getCurrentSetting("sqlite_all_varchar").getValue<bool>()) {
        for (auto i = 0u; i < columnTypes.size(); i++) {
            columnTypes[i] = common::LogicalType::STRING();
        }
    }
    return columnTypes;
}

} // namespace sqlite_extension
} // namespace kuzu
