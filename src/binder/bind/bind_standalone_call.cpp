#include "binder/binder.h"
#include "binder/call/bound_standalone_call.h"
#include "parser/call/standalone_call.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCall(const parser::Statement& statement) {
    auto& callStatement = reinterpret_cast<const parser::StandaloneCall&>(statement);
    auto option = main::DBConfig::getOptionByName(callStatement.getOptionName());
    if (option == nullptr) {
        throw common::BinderException{
            "Invalid option name: " + callStatement.getOptionName() + "."};
    }
    auto optionValue = expressionBinder.bindLiteralExpression(*callStatement.getOptionValue());
    // TODO(Ziyi): add casting rule for option value.
    ExpressionBinder::validateExpectedDataType(*optionValue, option->parameterType);
    return std::make_unique<BoundStandaloneCall>(*option, std::move(optionValue));
}

} // namespace binder
} // namespace kuzu
