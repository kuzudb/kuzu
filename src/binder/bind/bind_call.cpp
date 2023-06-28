#include "binder/binder.h"
#include "binder/call/bound_call_config.h"
#include "binder/call/bound_call_table_func.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/variable_expression.h"
#include "parser/call/call.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCallTableFunc(const parser::Statement& statement) {
    auto& callStatement = reinterpret_cast<const parser::Call&>(statement);
    auto tableFunctionDefinition =
        catalog.getBuiltInTableOperation()->mathTableOperation(callStatement.getOptionName());
    auto boundExpr = expressionBinder.bindLiteralExpression(*callStatement.getOptionValue());
    auto inputValue = reinterpret_cast<LiteralExpression*>(boundExpr.get())->getValue();
    auto bindData = tableFunctionDefinition->bindFunc(
        function::TableFuncBindInput{std::vector<common::Value>{*inputValue}},
        catalog.getReadOnlyVersion());
    auto statementResult = std::make_unique<BoundStatementResult>();
    for (auto i = 0u; i < bindData->returnColumnNames.size(); i++) {
        auto expr = std::make_shared<VariableExpression>(bindData->returnTypes[i],
            bindData->returnColumnNames[i], bindData->returnColumnNames[i]);
        statementResult->addColumn(expr, expression_vector{expr});
    }
    return std::make_unique<BoundCallTableFunc>(
        tableFunctionDefinition->tableFunc, std::move(bindData), std::move(statementResult));
}

std::unique_ptr<BoundStatement> Binder::bindCallConfig(const parser::Statement& statement) {
    auto& callStatement = reinterpret_cast<const parser::Call&>(statement);
    auto option = main::DBConfig::getOptionByName(callStatement.getOptionName());
    if (option == nullptr) {
        throw common::BinderException{
            "Invalid option name: " + callStatement.getOptionName() + "."};
    }
    auto optionValue = expressionBinder.bindLiteralExpression(*callStatement.getOptionValue());
    // TODO(Ziyi): add casting rule for option value.
    ExpressionBinder::validateExpectedDataType(*optionValue, option->parameterType);
    return std::make_unique<BoundCallConfig>(*option, std::move(optionValue));
}

} // namespace binder
} // namespace kuzu
