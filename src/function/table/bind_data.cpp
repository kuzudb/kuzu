#include "function/table/bind_data.h"

namespace kuzu {
namespace function {

std::vector<bool> TableFuncBindData::getColumnSkips() const {
    if (columnSkips.empty()) {
        // If not specified, all columns should be scanned.
        return std::vector<bool>(getNumColumns(), false);
    }
    return columnSkips;
}

} // namespace function
} // namespace kuzu
