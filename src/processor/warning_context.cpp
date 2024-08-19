#include "processor/warning_context.h"

#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::vector<std::string> WarningSchema::getColumnNames() {
    static constexpr std::initializer_list<const char*> names = {"query_id", "message", "file_path",
        "line_number", "reconstructed_line"};
    static_assert(getNumColumns() == names.size());
    std::vector<std::string> ret;
    for (const auto name : names) {
        ret.emplace_back(name);
    }
    return ret;
}

std::vector<common::LogicalType> WarningSchema::getColumnDataTypes() {
    std::vector<common::LogicalType> ret;
    ret.push_back(common::LogicalType::UINT64());
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::UINT64());
    ret.push_back(common::LogicalType::STRING());
    KU_ASSERT(getNumColumns() == ret.size());

    return common::LogicalType::copy(ret);
}

void WarningContext::appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
    uint64_t messageLimit) {
    common::UniqLock lock{mtx};
    warningLimit = messageLimit;

    warnings.insert(warnings.end(), messages.begin(), messages.end());

    KU_ASSERT(warnings.size() <= warningLimit);
}

} // namespace processor
} // namespace kuzu
