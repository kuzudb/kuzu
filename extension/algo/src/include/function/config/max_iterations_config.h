#pragma once

#include <string>

#include "common/types/types.h"
#include "function/table/optional_params.h"

namespace kuzu {
namespace algo_extension {

// The maximum number of iterations to run.
struct MaxIterations {
    static constexpr const char* NAME = "maxiterations";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 20;

    static void validate(int64_t maxIterations);
};

struct MaxIterationOptionalParams : public function::OptionalParams {
    function::OptionalParam<MaxIterations> maxIterations;

    explicit MaxIterationOptionalParams(const binder::expression_vector& optionalParams);

    // For copy only
    explicit MaxIterationOptionalParams(function::OptionalParam<MaxIterations> maxIterations)
        : maxIterations{std::move(maxIterations)} {}

    static binder::expression_vector constructMaxIterationParam(
        const binder::expression_vector& optionalParams);

    void evaluateParams(main::ClientContext* context) override {
        maxIterations.evaluateParam(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<MaxIterationOptionalParams>(maxIterations);
    }
};

} // namespace algo_extension
} // namespace kuzu
