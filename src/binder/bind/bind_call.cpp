#include "binder/binder.h"
#include "binder/call/bound_call.h"
#include "common/string_utils.h"
#include "parser/call/call.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCallClause(const parser::Statement& statement) {
    auto& callStatement = reinterpret_cast<const parser::Call&>(statement);
    auto option = main::DBConfig::getOptionByName(callStatement.getOptionName());
    if (option == nullptr) {
        throw common::BinderException{
            "Invalid option name: " + callStatement.getOptionName() + "."};
    }
    auto optionValue = expressionBinder.bindLiteralExpression(*callStatement.getOptionValue());
    // TODO(Ziyi): add casting rule for option value.
    ExpressionBinder::validateExpectedDataType(*optionValue, option->parameterType);
    auto boundCall = std::make_unique<BoundCall>(*option, std::move(optionValue));
    return boundCall;
}

} // namespace binder
} // namespace kuzu
