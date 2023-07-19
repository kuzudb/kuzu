#include "function/scalar_macro_function.h"

namespace kuzu {
namespace function {

macro_parameter_value_map ScalarMacroFunction::getDefaultParameterVals() const {
    macro_parameter_value_map defaultArgsToReturn;
    for (auto& defaultArg : defaultArgs) {
        defaultArgsToReturn.emplace(defaultArg.first, defaultArg.second.get());
    }
    return defaultArgsToReturn;
}

std::unique_ptr<ScalarMacroFunction> ScalarMacroFunction::copy() const {
    parser::default_macro_args defaultArgsCopy;
    for (auto& defaultArg : defaultArgs) {
        defaultArgsCopy.emplace_back(defaultArg.first, defaultArg.second->copy());
    }
    return std::make_unique<ScalarMacroFunction>(
        expression->copy(), positionalArgs, std::move(defaultArgsCopy));
}

} // namespace function
} // namespace kuzu
