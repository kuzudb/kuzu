#include "function/config/connected_components_config.h"

#include "binder/expression/expression_util.h"
#include "common/string_utils.h"
#include "function/config/max_iterations_config.h"

namespace kuzu {
namespace algo_extension {

using namespace function;
using namespace common;
using namespace binder;

CCOptionalParams::CCOptionalParams(const expression_vector& optionalParams) {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == MaxIterations::NAME) {
            maxIteration = optionalParam;
        } else {
            throw BinderException{"Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

std::unique_ptr<function::GDSConfig> CCOptionalParams::getConfig() const {
    auto config = std::make_unique<CCConfig>();
    if (maxIteration != nullptr) {
        config->maxIterations = ExpressionUtil::evaluateLiteral<int64_t>(*maxIteration,
            LogicalType::INT64(), MaxIterations::validate);
    }
    return config;
}

} // namespace algo_extension
} // namespace kuzu
