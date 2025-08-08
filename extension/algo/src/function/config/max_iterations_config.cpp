#include "function/config/max_iterations_config.h"

#include "common/exception/binder.h"
#include "common/string_utils.h"

namespace kuzu {
namespace algo_extension {

using namespace function;
using namespace common;
using namespace binder;

void MaxIterations::validate(int64_t maxIterations) {
    if (maxIterations < 0) {
        throw common::BinderException{"Max iteration must be a positive integer."};
    }
}

MaxIterationOptionalParams::MaxIterationOptionalParams(
    const binder::expression_vector& optionalParams) {
    for (auto& optionalParam : optionalParams) {
        auto paramName = common::StringUtils::getLower(optionalParam->getAlias());
        if (paramName == MaxIterations::NAME) {
            maxIterations = function::OptionalParam<MaxIterations>(optionalParam);
        } else {
            throw common::BinderException{
                "Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

expression_vector MaxIterationOptionalParams::constructMaxIterationParam(
    const expression_vector& optionalParams) {
    expression_vector maxIterationsParams;
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == MaxIterations::NAME) {
            maxIterationsParams.push_back(optionalParam);
        }
    }
    return maxIterationsParams;
}

} // namespace algo_extension
} // namespace kuzu
