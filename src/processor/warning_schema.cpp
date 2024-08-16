#include "processor/warning_schema.h"

namespace kuzu {
namespace processor {

std::vector<std::string> WarningSchema::getColumnNames() {
    static constexpr std::initializer_list<const char*> names = {"Message", "File Path",
        "Line Number", "Reconstructed Line"};
    static_assert(getNumColumns() == names.size());
    std::vector<std::string> ret;
    for (const auto name : names) {
        ret.emplace_back(name);
    }
    return ret;
}

std::vector<common::LogicalType> WarningSchema::getColumnDataTypes() {
    std::vector<common::LogicalType> ret;
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::UINT64());
    ret.push_back(common::LogicalType::STRING());
    KU_ASSERT(getNumColumns() == ret.size());

    return common::LogicalType::copy(ret);
}

} // namespace processor
} // namespace kuzu
