#pragma once

#include "binder/expression/expression.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace gds_extension {

struct CCConfig final : public function::GDSConfig {
    uint64_t maxIterations = 100;

    CCConfig() = default;
};

static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

struct CCOptionalParams final : public function::GDSOptionalParams {
    std::shared_ptr<binder::Expression> maxIteration;

    explicit CCOptionalParams(const binder::expression_vector& optionalParams);

    std::unique_ptr<function::GDSConfig> getConfig() const override;

    std::unique_ptr<GDSOptionalParams> copy() const override {
        return std::make_unique<CCOptionalParams>(*this);
    }
};

} // namespace gds_extension
} // namespace kuzu
