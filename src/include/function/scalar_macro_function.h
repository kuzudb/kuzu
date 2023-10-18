#pragma once

#include <unordered_map>

#include "parser/create_macro.h"

namespace kuzu {
namespace function {

using macro_parameter_value_map = std::unordered_map<std::string, parser::ParsedExpression*>;

struct ScalarMacroFunction {
    std::unique_ptr<parser::ParsedExpression> expression;
    std::vector<std::string> positionalArgs;
    parser::default_macro_args defaultArgs;

    ScalarMacroFunction(std::unique_ptr<parser::ParsedExpression> expression,
        std::vector<std::string> positionalArgs, parser::default_macro_args defaultArgs)
        : expression{std::move(expression)}, positionalArgs{std::move(positionalArgs)},
          defaultArgs{std::move(defaultArgs)} {}

    inline std::string getDefaultParameterName(uint64_t idx) const {
        return defaultArgs[idx].first;
    }

    inline uint64_t getNumArgs() const { return positionalArgs.size() + defaultArgs.size(); }

    std::vector<std::string> getPositionalArgs() const { return positionalArgs; }

    macro_parameter_value_map getDefaultParameterVals() const;

    std::unique_ptr<ScalarMacroFunction> copy() const;

    void serialize(common::Serializer& serializer) const;

    static std::unique_ptr<ScalarMacroFunction> deserialize(common::Deserializer& deserializer);
};

} // namespace function
} // namespace kuzu
