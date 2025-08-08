#pragma once

#include "function/gds/gds.h"

namespace kuzu {
namespace algo_extension {

struct CCConfig final : public function::GDSConfig {
    uint64_t maxIterations = 100;

    CCConfig() = default;
};

static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

} // namespace algo_extension
} // namespace kuzu
