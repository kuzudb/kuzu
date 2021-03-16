#include "src/parser/include/transformer.h"

namespace graphflow {
namespace parser {

unique_ptr<SingleQuery> Transformer::transformParseTree(CypherParser::OC_CypherContext* ctx) {
    return transformQuery(ctx->oC_Statement()->oC_Query());
}

unique_ptr<SingleQuery> Transformer::transformQuery(CypherParser::OC_QueryContext* ctx) {
    return transformRegularQuery(ctx->oC_RegularQuery());
}

unique_ptr<SingleQuery> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext* ctx) {
    return transformSingleQuery(ctx->oC_SingleQuery());
}

unique_ptr<SingleQuery> Transformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext* ctx) {
    return transformSinglePartQuery(ctx->oC_SinglePartQuery());
}

unique_ptr<SingleQuery> Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext* ctx) {
    auto singleQuery = make_unique<SingleQuery>();
    auto readingClauses = ctx->oC_ReadingClause();
    for (auto it = begin(readingClauses); it != end(readingClauses); ++it) {
        singleQuery->addMatchStatement(move(transformReadingClause(*it)));
    }
    return singleQuery;
}

unique_ptr<MatchStatement> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext* ctx) {
    return transformMatch(ctx->oC_Match());
}

unique_ptr<MatchStatement> Transformer::transformMatch(CypherParser::OC_MatchContext* ctx) {
    auto matchStatement = make_unique<MatchStatement>(move(transformPattern(ctx->oC_Pattern())));
    if (ctx->oC_Where()) {
        matchStatement->setWhereClause(move(transformWhere(ctx->oC_Where())));
    }
    return matchStatement;
}

unique_ptr<ParsedExpression> Transformer::transformWhere(CypherParser::OC_WhereContext* ctx) {
    return transformExpression(ctx->oC_Expression());
}

vector<unique_ptr<PatternElement>> Transformer::transformPattern(
    CypherParser::OC_PatternContext* ctx) {
    vector<unique_ptr<PatternElement>> pattern;
    auto patternPartContext = ctx->oC_PatternPart();
    for (auto it = begin(patternPartContext); it != end(patternPartContext); ++it) {
        pattern.push_back(move(transformPatternPart(*it)));
    }
    return pattern;
}

unique_ptr<PatternElement> Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext* ctx) {
    return transformAnonymousPatternPart(ctx->oC_AnonymousPatternPart());
}

unique_ptr<PatternElement> Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext* ctx) {
    return transformPatternElement(ctx->oC_PatternElement());
}

unique_ptr<PatternElement> Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext* ctx) {
    if (ctx->oC_PatternElement()) {
        return transformPatternElement(ctx->oC_PatternElement());
    }
    auto patternElement = make_unique<PatternElement>();
    patternElement->setNodePattern(move(transformNodePattern(ctx->oC_NodePattern())));
    if (!ctx->oC_PatternElementChain().empty()) {
        auto patternElementChainContext = ctx->oC_PatternElementChain();
        for (auto it = begin(patternElementChainContext); it != end(patternElementChainContext);
             ++it) {
            patternElement->addPatternElementChain(move(transformPatternElementChain(*it)));
        }
    }
    return patternElement;
}

unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext* ctx) {
    auto nodePattern = make_unique<NodePattern>();
    if (ctx->oC_Variable()) {
        nodePattern->setName(transformSymbolicName(ctx->oC_Variable()->oC_SymbolicName()));
    }
    if (ctx->oC_NodeLabel()) {
        nodePattern->setLabel(transformNodeLabel(ctx->oC_NodeLabel()));
    }
    return nodePattern;
}

unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext* ctx) {
    auto patternElementChain = make_unique<PatternElementChain>();
    patternElementChain->setRelPattern(
        move(transformRelationshipPattern(ctx->oC_RelationshipPattern())));
    patternElementChain->setNodePattern(move(transformNodePattern(ctx->oC_NodePattern())));
    return patternElementChain;
}

unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext* ctx) {
    auto relPattern = ctx->oC_RelationshipDetail() ?
                          transformRelationshipDetail(ctx->oC_RelationshipDetail()) :
                          make_unique<RelPattern>();
    relPattern->setDirection(
        ctx->oC_LeftArrowHead() ? ArrowDirection::LEFT : ArrowDirection::RIGHT);
    return relPattern;
}

unique_ptr<RelPattern> Transformer::transformRelationshipDetail(
    CypherParser::OC_RelationshipDetailContext* ctx) {
    auto relPattern = make_unique<RelPattern>();
    if (ctx->oC_Variable()) {
        relPattern->setName(transformVariable(ctx->oC_Variable()));
    }
    if (ctx->oC_RelTypeName()) {
        relPattern->setType(transformRelTypeName(ctx->oC_RelTypeName()));
    }
    return relPattern;
}

string Transformer::transformNodeLabel(CypherParser::OC_NodeLabelContext* ctx) {
    return transformLabelName(ctx->oC_LabelName());
}

string Transformer::transformLabelName(CypherParser::OC_LabelNameContext* ctx) {
    return transformSchemaName(ctx->oC_SchemaName());
}

string Transformer::transformRelTypeName(CypherParser::OC_RelTypeNameContext* ctx) {
    return transformSchemaName(ctx->oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformExpression(
    CypherParser::OC_ExpressionContext* ctx) {
    return transformOrExpression(ctx->oC_OrExpression());
}

unique_ptr<ParsedExpression> Transformer::transformOrExpression(
    CypherParser::OC_OrExpressionContext* ctx) {
    unique_ptr<ParsedExpression> result;
    if (1 == ctx->oC_XorExpression().size()) {
        return transformXorExpression(ctx->oC_XorExpression(0));
    }
    auto expression = make_unique<ParsedExpression>(ExpressionType::OR);
    auto xorExpression = ctx->oC_XorExpression();
    for (auto it = begin(xorExpression); it != end(xorExpression); ++it) {
        expression->addChild(move(transformXorExpression(*it)));
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformXorExpression(
    CypherParser::OC_XorExpressionContext* ctx) {
    unique_ptr<ParsedExpression> result;
    if (1 == ctx->oC_AndExpression().size()) {
        return transformAndExpression(ctx->oC_AndExpression(0));
    }
    auto expression = make_unique<ParsedExpression>(ExpressionType::XOR);
    auto andExpressionContext = ctx->oC_AndExpression();
    for (auto it = begin(andExpressionContext); it != end(andExpressionContext); ++it) {
        expression->addChild(move(transformAndExpression(*it)));
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAndExpression(
    CypherParser::OC_AndExpressionContext* ctx) {
    if (1 == ctx->oC_NotExpression().size()) {
        return transformNotExpression(ctx->oC_NotExpression(0));
    }
    auto expression = make_unique<ParsedExpression>(ExpressionType::AND);
    auto propertyExpressionContext = ctx->oC_NotExpression();
    for (auto it = begin(propertyExpressionContext); it != end(propertyExpressionContext); ++it) {
        expression->addChild(move(transformNotExpression(*it)));
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext* ctx) {
    if (ctx->NOT()) {
        auto expression = make_unique<ParsedExpression>(ExpressionType::NOT);
        expression->addChild(move(transformComparisonExpression(ctx->oC_ComparisonExpression())));
        return expression;
    }
    return transformComparisonExpression(ctx->oC_ComparisonExpression());
}

unique_ptr<ParsedExpression> Transformer::transformComparisonExpression(
    CypherParser::OC_ComparisonExpressionContext* ctx) {
    if (1 == ctx->oC_AddOrSubtractExpression().size()) {
        return transformAddOrSubtractExpression(ctx->oC_AddOrSubtractExpression(0));
    }
    unique_ptr<ParsedExpression> expression;
    auto comparisonOperator = ctx->gF_ComparisonOperator()->getText();
    if (comparisonOperator == "=") {
        expression = make_unique<ParsedExpression>(ExpressionType::EQUALS);
    } else if (comparisonOperator == "<>") {
        expression = make_unique<ParsedExpression>(ExpressionType::NOT_EQUALS);
    } else if (comparisonOperator == ">") {
        expression = make_unique<ParsedExpression>(ExpressionType::GREATER_THAN);
    } else if (comparisonOperator == ">=") {
        expression = make_unique<ParsedExpression>(ExpressionType::GREATER_THAN_EQUALS);
    } else if (comparisonOperator == "<") {
        expression = make_unique<ParsedExpression>(ExpressionType::LESS_THAN);
    } else if (comparisonOperator == "<=") {
        expression = make_unique<ParsedExpression>(ExpressionType::LESS_THAN_EQUALS);
    } else {
        throw invalid_argument("Unable to parse ComparisonExpressionContext.");
    }
    expression->addChild(
        move(transformAddOrSubtractExpression(ctx->oC_AddOrSubtractExpression(0))));
    expression->addChild(
        move(transformAddOrSubtractExpression(ctx->oC_AddOrSubtractExpression(1))));
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext* ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx->oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next =
            transformMultiplyDivideModuloExpression(ctx->oC_MultiplyDivideModuloExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            auto arithmeticOperator = ctx->gF_AddOrSubtractOperator(i - 1)->getText();
            if (arithmeticOperator == "+") {
                expression = make_unique<ParsedExpression>(
                    ExpressionType::ADD, move(expression), move(next));
            } else if (arithmeticOperator == "-") {
                expression = make_unique<ParsedExpression>(
                    ExpressionType::SUBTRACT, move(expression), move(next));
            } else {
                throw invalid_argument("Unable to parse AddOrSubtractExpressionContext");
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformMultiplyDivideModuloExpression(
    CypherParser::OC_MultiplyDivideModuloExpressionContext* ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx->oC_PowerOfExpression().size(); i++) {
        auto next = transformPowerOfExpression(ctx->oC_PowerOfExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            auto arithmeticOperator = ctx->gF_MultiplyDivideModuloOperator(i - 1)->getText();
            if (arithmeticOperator == "*") {
                expression = make_unique<ParsedExpression>(
                    ExpressionType::MULTIPLY, move(expression), move(next));
            } else if (arithmeticOperator == "/") {
                expression = make_unique<ParsedExpression>(
                    ExpressionType::DIVIDE, move(expression), move(next));
            } else if (arithmeticOperator == "%") {
                expression = make_unique<ParsedExpression>(
                    ExpressionType::MODULO, move(expression), move(next));
            } else {
                throw invalid_argument("Unable to parse MultiplyDivideModuloExpressionContext.");
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext* ctx) {
    unique_ptr<ParsedExpression> expression;
    auto unaryAddOrSubtractExpressionContext = ctx->oC_UnaryAddOrSubtractExpression();
    for (auto it = begin(unaryAddOrSubtractExpressionContext);
         it != end(unaryAddOrSubtractExpressionContext); ++it) {
        auto next = transformUnaryAddOrSubtractExpression(*it);
        if (!expression) {
            expression = move(next);
        } else {
            expression =
                make_unique<ParsedExpression>(ExpressionType::POWER, move(expression), move(next));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformUnaryAddOrSubtractExpression(
    CypherParser::OC_UnaryAddOrSubtractExpressionContext* ctx) {
    if (ctx->MINUS()) {
        auto expression = make_unique<ParsedExpression>(ExpressionType::NEGATE);
        expression->addChild(move(
            transformStringListNullOperatorExpression(ctx->oC_StringListNullOperatorExpression())));
        return expression;
    }
    return transformStringListNullOperatorExpression(ctx->oC_StringListNullOperatorExpression());
}

unique_ptr<ParsedExpression> Transformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext* ctx) {
    auto propertyExpression =
        transformPropertyOrLabelsExpression(ctx->oC_PropertyOrLabelsExpression());
    if (ctx->oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(
            ctx->oC_NullOperatorExpression(), move(propertyExpression));
    }
    if (ctx->oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(
            ctx->oC_StringOperatorExpression(), move(propertyExpression));
    }
    return propertyExpression;
}

unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext* ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    unique_ptr<ParsedExpression> expression;
    if (ctx->STARTS()) {
        expression = make_unique<ParsedExpression>(ExpressionType::STARTS_WITH);
    } else if (ctx->ENDS()) {
        expression = make_unique<ParsedExpression>(ExpressionType::ENDS_WITH);
    } else if (ctx->CONTAINS()) {
        expression = make_unique<ParsedExpression>(ExpressionType::CONTAINS);
    } else {
        throw invalid_argument("Unable to parse StringOperatorExpressionContext.");
    }
    expression->addChild(move(propertyExpression));
    expression->addChild(
        move(transformPropertyOrLabelsExpression(ctx->oC_PropertyOrLabelsExpression())));
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext* ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    if (ctx->IS() && ctx->NULL_()) {
        auto expression = ctx->NOT() ? make_unique<ParsedExpression>(ExpressionType::IS_NOT_NULL) :
                                       make_unique<ParsedExpression>(ExpressionType::IS_NULL);
        expression->addChild(move(propertyExpression));
        return expression;
    } else {
        throw invalid_argument("Unable to parse NullOperatorExpressionContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext* ctx) {
    if (ctx->oC_PropertyLookup()) {
        auto expression = make_unique<ParsedExpression>(
            ExpressionType::PROPERTY, transformPropertyLookup(ctx->oC_PropertyLookup()));
        expression->addChild(move(transformAtom(ctx->oC_Atom())));
        return expression;
    }
    return transformAtom(ctx->oC_Atom());
}

unique_ptr<ParsedExpression> Transformer::transformAtom(CypherParser::OC_AtomContext* ctx) {
    if (ctx->oC_Literal()) {
        return transformLiteral(ctx->oC_Literal());
    } else if (ctx->oC_ParenthesizedExpression()) {
        return transformParenthesizedExpression(ctx->oC_ParenthesizedExpression());
    } else if (ctx->oC_FunctionInvocation()) {
        return transformFunctionInvocation(ctx->oC_FunctionInvocation());
    } else if (ctx->oC_Variable()) {
        return make_unique<ParsedExpression>(
            ExpressionType::VARIABLE, move(transformVariable(ctx->oC_Variable())));
    } else {
        throw invalid_argument("Unable to parse AtomContext");
    }
}

unique_ptr<ParsedExpression> Transformer::transformLiteral(CypherParser::OC_LiteralContext* ctx) {
    if (ctx->oC_NumberLiteral()) {
        return transformNumberLiteral(ctx->oC_NumberLiteral());
    } else if (ctx->oC_BooleanLiteral()) {
        return transformBooleanLiteral(ctx->oC_BooleanLiteral());
    } else if (ctx->StringLiteral()) {
        return make_unique<ParsedExpression>(
            ExpressionType::LITERAL_STRING, ctx->StringLiteral()->getText());
    } else if (ctx->NULL_()) {
        return make_unique<ParsedExpression>(ExpressionType::LITERAL_NULL);
    } else {
        throw invalid_argument("Unable to parse LiteralContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext* ctx) {
    if (ctx->TRUE()) {
        return make_unique<ParsedExpression>(ExpressionType::LITERAL_BOOLEAN, "true");
    } else if (ctx->FALSE()) {
        return make_unique<ParsedExpression>(ExpressionType::LITERAL_BOOLEAN, "false");
    } else {
        throw invalid_argument("Unable to parse BooleanLiteralContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext* ctx) {
    return transformExpression(ctx->oC_Expression());
}

unique_ptr<ParsedExpression> Transformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext* ctx) {
    auto expression = make_unique<ParsedExpression>(
        ExpressionType::FUNCTION, transformFunctionName(ctx->oC_FunctionName()));
    if (!ctx->oC_Expression().empty()) {
        auto parameterContext = ctx->oC_Expression();
        for (auto it = begin(parameterContext); it != end(parameterContext); ++it) {
            expression->addChild(transformExpression(*it));
        }
    }
    return expression;
}

string Transformer::transformFunctionName(CypherParser::OC_FunctionNameContext* ctx) {
    return transformSymbolicName(ctx->oC_SymbolicName());
}

string Transformer::transformPropertyLookup(CypherParser::OC_PropertyLookupContext* ctx) {
    return transformPropertyKeyName(ctx->oC_PropertyKeyName());
}

string Transformer::transformVariable(CypherParser::OC_VariableContext* ctx) {
    return transformSymbolicName(ctx->oC_SymbolicName());
}

unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext* ctx) {
    if (ctx->oC_IntegerLiteral()) {
        return transformIntegerLiteral(ctx->oC_IntegerLiteral());
    } else if (ctx->oC_DoubleLiteral()) {
        return transformDoubleLiteral(ctx->oC_DoubleLiteral());
    } else {
        throw invalid_argument("Unable to parse NumberLiteralContext.");
    }
}

string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext* ctx) {
    return transformSchemaName(ctx->oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(
    CypherParser::OC_IntegerLiteralContext* ctx) {
    return make_unique<ParsedExpression>(
        ExpressionType::LITERAL_INT, ctx->DecimalInteger()->getText());
}

unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(
    CypherParser::OC_DoubleLiteralContext* ctx) {
    return make_unique<ParsedExpression>(
        ExpressionType::LITERAL_DOUBLE, ctx->RegularDecimalReal()->getText());
}

string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext* ctx) {
    return transformSymbolicName(ctx->oC_SymbolicName());
}

string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext* ctx) {
    if (ctx->UnescapedSymbolicName()) {
        return ctx->UnescapedSymbolicName()->getText();
    } else if (ctx->EscapedSymbolicName()) {
        return ctx->EscapedSymbolicName()->getText();
    } else if (ctx->HexLetter()) {
        return ctx->HexLetter()->getText();
    } else {
        throw invalid_argument("Unable to parse SymbolicNameContext.");
    }
}

} // namespace parser
} // namespace graphflow
