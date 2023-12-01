#include "function/cast/functions/cast_from_string_functions.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/expression/parsed_parameter_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_subquery_expression.h"
#include "parser/expression/parsed_variable_expression.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedExpression> Transformer::transformExpression(
    CypherParser::OC_ExpressionContext& ctx) {
    return transformOrExpression(*ctx.oC_OrExpression());
}

std::unique_ptr<ParsedExpression> Transformer::transformOrExpression(
    CypherParser::OC_OrExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& xorExpression : ctx.oC_XorExpression()) {
        auto next = transformXorExpression(*xorExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " OR " + next->getRawName();
            expression = std::make_unique<ParsedExpression>(
                ExpressionType::OR, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformXorExpression(
    CypherParser::OC_XorExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& andExpression : ctx.oC_AndExpression()) {
        auto next = transformAndExpression(*andExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " XOR " + next->getRawName();
            expression = std::make_unique<ParsedExpression>(
                ExpressionType::XOR, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformAndExpression(
    CypherParser::OC_AndExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& notExpression : ctx.oC_NotExpression()) {
        auto next = transformNotExpression(*notExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " AND " + next->getRawName();
            expression = std::make_unique<ParsedExpression>(
                ExpressionType::AND, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext& ctx) {
    if (ctx.NOT()) {
        return std::make_unique<ParsedExpression>(ExpressionType::NOT,
            transformComparisonExpression(*ctx.oC_ComparisonExpression()), ctx.getText());
    }
    return transformComparisonExpression(*ctx.oC_ComparisonExpression());
}

std::unique_ptr<ParsedExpression> Transformer::transformComparisonExpression(
    CypherParser::OC_ComparisonExpressionContext& ctx) {
    if (1 == ctx.kU_BitwiseOrOperatorExpression().size()) {
        return transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    }
    // Antlr parser throws error for conjunctive comparison.
    // Transformer should only handle the case of single comparison operator.
    KU_ASSERT(ctx.kU_ComparisonOperator().size() == 1);
    auto left = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    auto right = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(1));
    auto comparisonOperator = ctx.kU_ComparisonOperator()[0]->getText();
    if (comparisonOperator == "=") {
        return std::make_unique<ParsedExpression>(
            ExpressionType::EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<>") {
        return std::make_unique<ParsedExpression>(
            ExpressionType::NOT_EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">") {
        return std::make_unique<ParsedExpression>(
            ExpressionType::GREATER_THAN, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">=") {
        return std::make_unique<ParsedExpression>(
            ExpressionType::GREATER_THAN_EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<") {
        return std::make_unique<ParsedExpression>(
            ExpressionType::LESS_THAN, std::move(left), std::move(right), ctx.getText());
    } else {
        KU_ASSERT(comparisonOperator == "<=");
        return std::make_unique<ParsedExpression>(
            ExpressionType::LESS_THAN_EQUALS, std::move(left), std::move(right), ctx.getText());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformBitwiseOrOperatorExpression(
    CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.kU_BitwiseAndOperatorExpression().size(); ++i) {
        auto next = transformBitwiseAndOperatorExpression(*ctx.kU_BitwiseAndOperatorExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " | " + next->getRawName();
            expression = std::make_unique<ParsedFunctionExpression>(
                BITWISE_OR_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformBitwiseAndOperatorExpression(
    CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.kU_BitShiftOperatorExpression().size(); ++i) {
        auto next = transformBitShiftOperatorExpression(*ctx.kU_BitShiftOperatorExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " & " + next->getRawName();
            expression = std::make_unique<ParsedFunctionExpression>(
                BITWISE_AND_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformBitShiftOperatorExpression(
    CypherParser::KU_BitShiftOperatorExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_AddOrSubtractExpression().size(); ++i) {
        auto next = transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto bitShiftOperator = ctx.kU_BitShiftOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + bitShiftOperator + " " + next->getRawName();
            if (bitShiftOperator == "<<") {
                expression = std::make_unique<ParsedFunctionExpression>(
                    BITSHIFT_LEFT_FUNC_NAME, std::move(expression), std::move(next), rawName);
            } else {
                KU_ASSERT(bitShiftOperator == ">>");
                expression = std::make_unique<ParsedFunctionExpression>(
                    BITSHIFT_RIGHT_FUNC_NAME, std::move(expression), std::move(next), rawName);
            }
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next =
            transformMultiplyDivideModuloExpression(*ctx.oC_MultiplyDivideModuloExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto arithmeticOperator = ctx.kU_AddOrSubtractOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + arithmeticOperator + " " + next->getRawName();
            expression = std::make_unique<ParsedFunctionExpression>(
                arithmeticOperator, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformMultiplyDivideModuloExpression(
    CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_PowerOfExpression().size(); i++) {
        auto next = transformPowerOfExpression(*ctx.oC_PowerOfExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto arithmeticOperator = ctx.kU_MultiplyDivideModuloOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + arithmeticOperator + " " + next->getRawName();
            expression = std::make_unique<ParsedFunctionExpression>(
                arithmeticOperator, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& unaryAddOrSubtractExpression : ctx.oC_UnaryAddSubtractOrFactorialExpression()) {
        auto next = transformUnaryAddSubtractOrFactorialExpression(*unaryAddOrSubtractExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " ^ " + next->getRawName();
            expression = std::make_unique<ParsedFunctionExpression>(
                POWER_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformUnaryAddSubtractOrFactorialExpression(
    CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx) {
    if (ctx.MINUS() && ctx.FACTORIAL()) {
        auto exp1 = std::make_unique<ParsedFunctionExpression>(FACTORIAL_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
        return std::make_unique<ParsedFunctionExpression>(
            NEGATE_FUNC_NAME, std::move(exp1), ctx.getText());
    } else if (ctx.MINUS()) {
        return std::make_unique<ParsedFunctionExpression>(NEGATE_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
    } else if (ctx.FACTORIAL()) {
        return std::make_unique<ParsedFunctionExpression>(FACTORIAL_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
    }
    return transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression());
}

std::unique_ptr<ParsedExpression> Transformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext& ctx) {
    auto propertyExpression =
        transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(
            *ctx.oC_NullOperatorExpression(), std::move(propertyExpression));
    }
    if (!ctx.oC_ListOperatorExpression().empty()) {
        auto result = transformListOperatorExpression(
            *ctx.oC_ListOperatorExpression(0), std::move(propertyExpression));
        for (auto i = 1u; i < ctx.oC_ListOperatorExpression().size(); ++i) {
            result = transformListOperatorExpression(
                *ctx.oC_ListOperatorExpression(i), std::move(result));
        }
        return result;
    }
    if (ctx.oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(
            *ctx.oC_StringOperatorExpression(), std::move(propertyExpression));
    }
    return propertyExpression;
}

std::unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.STARTS()) {
        return std::make_unique<ParsedFunctionExpression>(
            STARTS_WITH_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    } else if (ctx.ENDS()) {
        return std::make_unique<ParsedFunctionExpression>(
            ENDS_WITH_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    } else if (ctx.CONTAINS()) {
        return std::make_unique<ParsedFunctionExpression>(
            CONTAINS_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    } else {
        KU_ASSERT(ctx.oC_RegularExpression());
        return std::make_unique<ParsedFunctionExpression>(REGEXP_FULL_MATCH_FUNC_NAME,
            std::move(propertyExpression), std::move(right), rawExpression);
    }
}

static std::unique_ptr<ParsedLiteralExpression> getZeroLiteral() {
    auto literal = std::make_unique<Value>(0);
    return std::make_unique<ParsedLiteralExpression>(std::move(literal), "0");
}

std::unique_ptr<ParsedExpression> Transformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> childExpression) {
    auto rawExpression = childExpression->getRawName() + ctx.getText();
    std::unique_ptr<ParsedExpression> listOperator;
    if (ctx.kU_ListSliceOperatorExpression()) {
        listOperator = transformListSliceOperatorExpression(
            *ctx.kU_ListSliceOperatorExpression(), std::move(childExpression));
    } else {
        listOperator = transformListExtractOperatorExpression(
            *ctx.kU_ListExtractOperatorExpression(), std::move(childExpression));
    }
    return listOperator;
}

std::unique_ptr<ParsedExpression> Transformer::transformListSliceOperatorExpression(
    CypherParser::KU_ListSliceOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto listSlice =
        std::make_unique<ParsedFunctionExpression>(LIST_SLICE_FUNC_NAME, std::move(rawExpression));
    listSlice->addChild(std::move(propertyExpression));
    if (ctx.children[1]->getText() == ":") {
        listSlice->addChild(getZeroLiteral());
        // Parsing [:right] syntax.
        if (ctx.oC_Expression(0)) {
            listSlice->addChild(transformExpression(*ctx.oC_Expression(0)));
            // Parsing [:] syntax.
        } else {
            listSlice->addChild(getZeroLiteral());
        }
    } else {
        // Parsing [left:right] syntax.
        if (ctx.oC_Expression(1)) {
            listSlice->addChild(transformExpression(*ctx.oC_Expression(0)));
            listSlice->addChild(transformExpression(*ctx.oC_Expression(1)));
            // Parsing [left:] syntax.
        } else {
            listSlice->addChild(transformExpression(*ctx.oC_Expression(0)));
            listSlice->addChild(getZeroLiteral());
        }
    }
    return listSlice;
}

std::unique_ptr<ParsedExpression> Transformer::transformListExtractOperatorExpression(
    CypherParser::KU_ListExtractOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto listExtract = std::make_unique<ParsedFunctionExpression>(
        LIST_EXTRACT_FUNC_NAME, std::move(rawExpression));
    listExtract->addChild(std::move(propertyExpression));
    listExtract->addChild(transformExpression(*ctx.oC_Expression()));
    return listExtract;
}

std::unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    KU_ASSERT(ctx.IS() && ctx.NULL_());
    return ctx.NOT() ? std::make_unique<ParsedExpression>(ExpressionType::IS_NOT_NULL,
                           std::move(propertyExpression), rawExpression) :
                       std::make_unique<ParsedExpression>(
                           ExpressionType::IS_NULL, std::move(propertyExpression), rawExpression);
}

std::unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext& ctx) {
    auto atom = transformAtom(*ctx.oC_Atom());
    auto raw = ctx.oC_Atom()->getText();
    if (!ctx.oC_PropertyLookup().empty()) {
        auto lookUpCtx = ctx.oC_PropertyLookup(0);
        auto result = createPropertyExpression(*lookUpCtx, std::move(atom));
        for (auto i = 1u; i < ctx.oC_PropertyLookup().size(); ++i) {
            lookUpCtx = ctx.oC_PropertyLookup(i);
            result = createPropertyExpression(*lookUpCtx, std::move(result));
        }
        return result;
    }
    return atom;
}

std::unique_ptr<ParsedExpression> Transformer::transformAtom(CypherParser::OC_AtomContext& ctx) {
    if (ctx.oC_Literal()) {
        return transformLiteral(*ctx.oC_Literal());
    } else if (ctx.oC_Parameter()) {
        return transformParameterExpression(*ctx.oC_Parameter());
    } else if (ctx.oC_CaseExpression()) {
        return transformCaseExpression(*ctx.oC_CaseExpression());
    } else if (ctx.oC_ParenthesizedExpression()) {
        return transformParenthesizedExpression(*ctx.oC_ParenthesizedExpression());
    } else if (ctx.oC_FunctionInvocation()) {
        return transformFunctionInvocation(*ctx.oC_FunctionInvocation());
    } else if (ctx.oC_PathPatterns()) {
        return transformPathPattern(*ctx.oC_PathPatterns());
    } else if (ctx.oC_ExistSubquery()) {
        return transformExistSubquery(*ctx.oC_ExistSubquery());
    } else if (ctx.kU_CountSubquery()) {
        return transformCountSubquery(*ctx.kU_CountSubquery());
    } else {
        KU_ASSERT(ctx.oC_Variable());
        return std::make_unique<ParsedVariableExpression>(
            transformVariable(*ctx.oC_Variable()), ctx.getText());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformLiteral(
    CypherParser::OC_LiteralContext& ctx) {
    if (ctx.oC_NumberLiteral()) {
        return transformNumberLiteral(*ctx.oC_NumberLiteral());
    } else if (ctx.oC_BooleanLiteral()) {
        return transformBooleanLiteral(*ctx.oC_BooleanLiteral());
    } else if (ctx.StringLiteral()) {
        return std::make_unique<ParsedLiteralExpression>(
            std::make_unique<Value>(
                LogicalType{LogicalTypeID::STRING}, transformStringLiteral(*ctx.StringLiteral())),
            ctx.getText());
    } else if (ctx.NULL_()) {
        return std::make_unique<ParsedLiteralExpression>(
            std::make_unique<Value>(Value::createNullValue()), ctx.getText());
    } else if (ctx.kU_StructLiteral()) {
        return transformStructLiteral(*ctx.kU_StructLiteral());
    } else {
        KU_ASSERT(ctx.oC_ListLiteral());
        return transformListLiteral(*ctx.oC_ListLiteral());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext& ctx) {
    std::unique_ptr<Value> literal;
    if (ctx.TRUE()) {
        literal = std::make_unique<Value>(true);
    } else if (ctx.FALSE()) {
        literal = std::make_unique<Value>(false);
    }
    KU_ASSERT(literal);
    return std::make_unique<ParsedLiteralExpression>(std::move(literal), ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformListLiteral(
    CypherParser::OC_ListLiteralContext& ctx) {
    auto listCreation =
        std::make_unique<ParsedFunctionExpression>(LIST_CREATION_FUNC_NAME, ctx.getText());
    if (ctx.oC_Expression() == nullptr) { // empty list
        return listCreation;
    }
    listCreation->addChild(transformExpression(*ctx.oC_Expression()));
    for (auto& listEntry : ctx.kU_ListEntry()) {
        if (listEntry->oC_Expression() == nullptr) {
            auto nullValue = Value::createNullValue();
            listCreation->addChild(
                std::make_unique<ParsedLiteralExpression>(nullValue.copy(), nullValue.toString()));
        } else {
            listCreation->addChild(transformExpression(*listEntry->oC_Expression()));
        }
    }
    return listCreation;
}

std::unique_ptr<ParsedExpression> Transformer::transformStructLiteral(
    CypherParser::KU_StructLiteralContext& ctx) {
    auto structPack =
        std::make_unique<ParsedFunctionExpression>(STRUCT_PACK_FUNC_NAME, ctx.getText());
    for (auto& structField : ctx.kU_StructField()) {
        auto structExpr = transformExpression(*structField->oC_Expression());
        std::string alias;
        if (structField->oC_Expression()) {
            alias = transformSymbolicName(*structField->oC_SymbolicName());
        } else {
            alias = transformStringLiteral(*structField->StringLiteral());
        }
        structExpr->setAlias(alias);
        structPack->addChild(std::move(structExpr));
    }
    return structPack;
}

std::unique_ptr<ParsedExpression> Transformer::transformParameterExpression(
    CypherParser::OC_ParameterContext& ctx) {
    auto parameterName =
        ctx.oC_SymbolicName() ? ctx.oC_SymbolicName()->getText() : ctx.DecimalInteger()->getText();
    return std::make_unique<ParsedParameterExpression>(parameterName, ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

std::unique_ptr<ParsedExpression> Transformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext& ctx) {
    if (ctx.STAR()) {
        return std::make_unique<ParsedFunctionExpression>(COUNT_STAR_FUNC_NAME, ctx.getText());
    }
    std::string functionName;
    if (ctx.COUNT()) {
        functionName = "COUNT";
    } else {
        functionName = transformFunctionName(*ctx.oC_FunctionName());
    }
    auto expression = std::make_unique<ParsedFunctionExpression>(
        functionName, ctx.getText(), ctx.DISTINCT() != nullptr);
    for (auto& functionParameter : ctx.kU_FunctionParameter()) {
        expression->addChild(transformFunctionParameterExpression(*functionParameter));
    }
    return expression;
}

std::string Transformer::transformFunctionName(CypherParser::OC_FunctionNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::unique_ptr<ParsedExpression> Transformer::transformFunctionParameterExpression(
    CypherParser::KU_FunctionParameterContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    if (ctx.oC_SymbolicName()) {
        expression->setAlias(transformSymbolicName(*ctx.oC_SymbolicName()));
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformPathPattern(
    CypherParser::OC_PathPatternsContext& ctx) {
    auto subquery = std::make_unique<ParsedSubqueryExpression>(SubqueryType::EXISTS, ctx.getText());
    auto patternElement =
        std::make_unique<PatternElement>(transformNodePattern(*ctx.oC_NodePattern()));
    for (auto& chain : ctx.oC_PatternElementChain()) {
        patternElement->addPatternElementChain(transformPatternElementChain(*chain));
    }
    subquery->addPatternElement(std::move(patternElement));
    return subquery;
}

std::unique_ptr<ParsedExpression> Transformer::transformExistSubquery(
    CypherParser::OC_ExistSubqueryContext& ctx) {
    auto subquery = std::make_unique<ParsedSubqueryExpression>(SubqueryType::EXISTS, ctx.getText());
    subquery->setPatternElements(transformPattern(*ctx.oC_Pattern()));
    if (ctx.oC_Where()) {
        subquery->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return subquery;
}

std::unique_ptr<ParsedExpression> Transformer::transformCountSubquery(
    CypherParser::KU_CountSubqueryContext& ctx) {
    auto subquery = std::make_unique<ParsedSubqueryExpression>(SubqueryType::COUNT, ctx.getText());
    subquery->setPatternElements(transformPattern(*ctx.oC_Pattern()));
    if (ctx.oC_Where()) {
        subquery->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return subquery;
}

std::unique_ptr<ParsedExpression> Transformer::createPropertyExpression(
    CypherParser::OC_PropertyLookupContext& ctx, std::unique_ptr<ParsedExpression> child) {
    auto key =
        ctx.STAR() ? InternalKeyword::STAR : transformPropertyKeyName(*ctx.oC_PropertyKeyName());
    return std::make_unique<ParsedPropertyExpression>(
        key, std::move(child), child->toString() + ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformCaseExpression(
    CypherParser::OC_CaseExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> caseExpression = nullptr;
    std::unique_ptr<ParsedExpression> elseExpression = nullptr;
    if (ctx.ELSE()) {
        if (ctx.oC_Expression().size() == 1) {
            elseExpression = transformExpression(*ctx.oC_Expression(0));
        } else {
            KU_ASSERT(ctx.oC_Expression().size() == 2);
            caseExpression = transformExpression(*ctx.oC_Expression(0));
            elseExpression = transformExpression(*ctx.oC_Expression(1));
        }
    } else {
        if (ctx.oC_Expression().size() == 1) {
            caseExpression = transformExpression(*ctx.oC_Expression(0));
        }
    }
    auto parsedCaseExpression = std::make_unique<ParsedCaseExpression>(ctx.getText());
    parsedCaseExpression->setCaseExpression(std::move(caseExpression));
    parsedCaseExpression->setElseExpression(std::move(elseExpression));
    for (auto& caseAlternative : ctx.oC_CaseAlternative()) {
        parsedCaseExpression->addCaseAlternative(transformCaseAlternative(*caseAlternative));
    }
    return parsedCaseExpression;
}

std::unique_ptr<ParsedCaseAlternative> Transformer::transformCaseAlternative(
    CypherParser::OC_CaseAlternativeContext& ctx) {
    auto whenExpression = transformExpression(*ctx.oC_Expression(0));
    auto thenExpression = transformExpression(*ctx.oC_Expression(1));
    return std::make_unique<ParsedCaseAlternative>(
        std::move(whenExpression), std::move(thenExpression));
}

std::unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext& ctx) {
    if (ctx.oC_IntegerLiteral()) {
        return transformIntegerLiteral(*ctx.oC_IntegerLiteral());
    } else {
        KU_ASSERT(ctx.oC_DoubleLiteral());
        return transformDoubleLiteral(*ctx.oC_DoubleLiteral());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformProperty(
    CypherParser::OC_PropertyExpressionContext& ctx) {
    auto child = transformAtom(*ctx.oC_Atom());
    return createPropertyExpression(*ctx.oC_PropertyLookup(), std::move(child));
}

std::string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

std::unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(
    CypherParser::OC_IntegerLiteralContext& ctx) {
    auto text = ctx.DecimalInteger()->getText();
    ku_string_t literal{text.c_str(), text.length()};
    int64_t result;
    if (function::CastString::tryCast(literal, result)) {
        return std::make_unique<ParsedLiteralExpression>(
            std::make_unique<Value>(result), ctx.getText());
    }
    int128_t result128;
    function::CastString::operation(literal, result128);
    return std::make_unique<ParsedLiteralExpression>(
        std::make_unique<Value>(result128), ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(
    CypherParser::OC_DoubleLiteralContext& ctx) {
    auto text = ctx.RegularDecimalReal()->getText();
    ku_string_t literal{text.c_str(), text.length()};
    double_t result;
    function::CastString::operation(literal, result);
    return std::make_unique<ParsedLiteralExpression>(
        std::make_unique<Value>(result), ctx.getText());
}

} // namespace parser
} // namespace kuzu
