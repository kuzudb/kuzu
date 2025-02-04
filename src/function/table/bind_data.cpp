#include "function/table/bind_data.h"

#include "common/constants.h"

namespace kuzu {
namespace function {

std::vector<bool> TableFuncBindData::getColumnSkips() const {
    if (columnSkips.empty()) { // If not specified, all columns should be scanned.
        std::vector<bool> skips;
        for (auto i = 0u; i < getNumColumns(); ++i) {
            skips.push_back(false);
        }
        return skips;
    }
    return columnSkips;
}

bool TableFuncBindData::getIgnoreErrorsOption() const {
    return common::CopyConstants::DEFAULT_IGNORE_ERRORS;
}

std::unique_ptr<TableFuncBindData> TableFuncBindData::copy() const {
    return std::make_unique<TableFuncBindData>(*this);
}

bool ScanBindData::getIgnoreErrorsOption() const {
    return fileScanInfo.getOption(common::CopyConstants::IGNORE_ERRORS_OPTION_NAME,
        common::CopyConstants::DEFAULT_IGNORE_ERRORS);
}

} // namespace function
} // namespace kuzu
