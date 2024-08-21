#include "function/table/bind_data.h"

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

} // namespace function
} // namespace kuzu
