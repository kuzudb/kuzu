#pragma once

#include "common/case_insensitive_map.h"
#include "common/types/value/value.h"

namespace kuzu {

struct PyScanConfig {
    uint64_t skipNum;
    uint64_t limitNum;
    bool ignoreErrors;
    explicit PyScanConfig(const common::case_insensitive_map_t<common::Value>& options,
        uint64_t numRows);
};

} // namespace kuzu
