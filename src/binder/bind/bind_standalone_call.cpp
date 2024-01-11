#include "binder/binder.h"
#include "binder/bound_standalone_call.h"
#include "common/exception/binder.h"
#include "extension/extension.h"
#include "main/db_config.h"
#include "parser/standalone_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCall(const parser::Statement& statement) {
    auto& callStatement =
        ku_dynamic_cast<const parser::Statement&, const parser::StandaloneCall&>(statement);
    main::Option* option = main::DBConfig::getOptionByName(callStatement.getOptionName());
    if (option == nullptr) {
        option = extensionOptions->getExtensionOption(callStatement.getOptionName());
    }
    if (option == nullptr) {
        throw BinderException{"Invalid option name: " + callStatement.getOptionName() + "."};
    }
    auto optionValue = expressionBinder.bindLiteralExpression(*callStatement.getOptionValue());
    // TODO(Ziyi): add casting rule for option value.
    ExpressionBinder::validateExpectedDataType(*optionValue, option->parameterType);
    return std::make_unique<BoundStandaloneCall>(option, std::move(optionValue));
}

} // namespace binder
} // namespace kuzu
