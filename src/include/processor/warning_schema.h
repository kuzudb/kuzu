#pragma once

#include <vector>

#include "common/types/types.h"

namespace kuzu {
namespace processor {

struct WarningSchema {
    static constexpr uint64_t getNumColumns() { return 4; }
    static std::vector<std::string> getColumnNames();
    static std::vector<common::LogicalType> getColumnDataTypes();
};

} // namespace processor
} // namespace kuzu
