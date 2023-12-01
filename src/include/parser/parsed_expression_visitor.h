#pragma once

#include <unordered_map>

#include "parser/expression/parsed_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"

namespace kuzu {
namespace parser {

class ParsedExpressionChildrenVisitor {
public:
    static std::vector<ParsedExpression*> collectChildren(const ParsedExpression& expression);

    static void setChild(kuzu::parser::ParsedExpression& expression, uint64_t idx,
        std::unique_ptr<ParsedExpression> expressionToSet);

private:
    static std::vector<ParsedExpression*> collectCaseChildren(const ParsedExpression& expression);

    static void setCaseChild(kuzu::parser::ParsedExpression& expression, uint64_t idx,
        std::unique_ptr<ParsedExpression> expressionToSet);
};

class MacroParameterReplacer {
public:
    explicit MacroParameterReplacer(
        const std::unordered_map<std::string, ParsedExpression*>& expressionNamesToReplace)
        : expressionNamesToReplace{expressionNamesToReplace} {}

    std::unique_ptr<ParsedExpression> visit(
        std::unique_ptr<ParsedExpression> macroExpression) const;

private:
    const std::unordered_map<std::string, ParsedExpression*>& expressionNamesToReplace;
};

class InQueryCallParameterReplacer {
public:
    explicit InQueryCallParameterReplacer(
        std::pair<std::string, ParsedLiteralExpression*> literalExpressionToReplace)
        : literalExpressionToReplace{std::move(literalExpressionToReplace)} {}

    ParsedFunctionExpression* visit(ParsedFunctionExpression* tableFuncExpression) const;

private:
    std::pair<std::string, ParsedLiteralExpression*> literalExpressionToReplace;
};

} // namespace parser
} // namespace kuzu
