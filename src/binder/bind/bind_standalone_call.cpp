#include "binder/binder.h"
#include "binder/bound_standalone_call.h"
#include "binder/expression/expression_util.h"
#include "binder/expression_visitor.h"
#include "common/cast.h"
#include "common/exception/binder.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "parser/standalone_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCall(const parser::Statement& statement) {
    auto& callStatement = ku_dynamic_cast<const parser::StandaloneCall&>(statement);
    const main::Option* option = main::DBConfig::getOptionByName(callStatement.getOptionName());
    if (option == nullptr) {
        option = clientContext->getExtensionOption(callStatement.getOptionName());
    }
    if (option == nullptr) {
        throw BinderException{"Invalid option name: " + callStatement.getOptionName() + "."};
    }
    auto optionValue = expressionBinder.bindExpression(*callStatement.getOptionValue());
    ExpressionUtil::validateExpressionType(*optionValue, ExpressionType::LITERAL);
    // Disallow casting from double->int64 for calls such as threads configuration.
    if (optionValue->dataType.getPhysicalType() == PhysicalTypeID::DOUBLE && LogicalType(option->parameterType).getPhysicalType() == PhysicalTypeID::INT64) {
        throw BinderException{"Invalid attempt at casting: From " + optionValue->dataType.toString() + " to " + LogicalType(option->parameterType).toString() + " during call " + callStatement.getOptionName() + "."};
    }
    optionValue =
        expressionBinder.implicitCastIfNecessary(optionValue, LogicalType(option->parameterType));
    if (ConstantExpressionVisitor::needFold(*optionValue)) {
        optionValue = expressionBinder.foldExpression(optionValue);
    }
    return std::make_unique<BoundStandaloneCall>(option, std::move(optionValue));
}

} // namespace binder
} // namespace kuzu
