#include "src/parser/include/transformer.h"

#include "src/parser/include/statements/load_csv_statement.h"
#include "src/parser/include/statements/match_statement.h"

namespace graphflow {
namespace parser {

unique_ptr<SingleQuery> Transformer::transform() {
    auto singleQuery = transformQuery(*root.oC_Statement()->oC_Query());
    if (root.oC_AnyCypherOption()) {
        auto cypherOption = root.oC_AnyCypherOption();
        singleQuery->enable_explain = cypherOption->oC_Explain() != nullptr;
        singleQuery->enable_profile = cypherOption->oC_Profile() != nullptr;
    }
    return singleQuery;
}

unique_ptr<SingleQuery> Transformer::transformQuery(CypherParser::OC_QueryContext& ctx) {
    return transformRegularQuery(*ctx.oC_RegularQuery());
}

unique_ptr<SingleQuery> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
    return transformSingleQuery(*ctx.oC_SingleQuery());
}

unique_ptr<SingleQuery> Transformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext& ctx) {
    vector<unique_ptr<QueryPart>> queryParts;
    if (ctx.oC_MultiPartQuery()) {
        for (auto queryPart : ctx.oC_MultiPartQuery()->gF_QueryPart()) {
            queryParts.push_back(transformQueryPart(*queryPart));
        }
    }
    auto singleQuery =
        ctx.oC_MultiPartQuery() ?
            transformSinglePartQuery(*ctx.oC_MultiPartQuery()->oC_SinglePartQuery()) :
            transformSinglePartQuery(*ctx.oC_SinglePartQuery());
    singleQuery->queryParts = move(queryParts);
    return singleQuery;
}

unique_ptr<SingleQuery> Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext& ctx) {
    auto singleQuery = make_unique<SingleQuery>();
    for (auto& readingClause : ctx.oC_ReadingClause()) {
        singleQuery->readingStatements.push_back(transformReadingClause(*readingClause));
    }
    singleQuery->returnStatement = transformReturn(*ctx.oC_Return());
    return singleQuery;
}

unique_ptr<QueryPart> Transformer::transformQueryPart(CypherParser::GF_QueryPartContext& ctx) {
    auto queryPart = make_unique<QueryPart>(transformWith(*ctx.oC_With()));
    for (auto& readingClause : ctx.oC_ReadingClause()) {
        queryPart->readingStatements.push_back(transformReadingClause(*readingClause));
    }
    return queryPart;
}

unique_ptr<ReadingStatement> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    return ctx.oC_Match() ? transformMatch(*ctx.oC_Match()) : transformLoadCSV(*ctx.oC_LoadCSV());
}

unique_ptr<ReadingStatement> Transformer::transformLoadCSV(CypherParser::OC_LoadCSVContext& ctx) {
    auto inputExpression = transformExpression(*ctx.oC_Expression());
    auto loadCSVStatement =
        make_unique<LoadCSVStatement>(move(inputExpression), transformVariable(*ctx.oC_Variable()));
    if (ctx.FIELDTERMINATOR()) {
        loadCSVStatement->fieldTerminator = ctx.StringLiteral()->getText();
    }
    return loadCSVStatement;
}

unique_ptr<ReadingStatement> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchStatement = make_unique<MatchStatement>(transformPattern(*ctx.oC_Pattern()));
    if (ctx.oC_Where()) {
        matchStatement->whereClause = transformWhere(*ctx.oC_Where());
    }
    return matchStatement;
}

unique_ptr<WithStatement> Transformer::transformWith(CypherParser::OC_WithContext& ctx) {
    auto projectionItems = ctx.oC_ProjectionBody()->oC_ProjectionItems();
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    for (auto& projectionItem : projectionItems->oC_ProjectionItem()) {
        expressions.push_back(transformProjectionItem(*projectionItem));
    }
    auto withStatement =
        make_unique<WithStatement>(move(expressions), nullptr != projectionItems->STAR());
    if (ctx.oC_Where()) {
        withStatement->whereClause = transformWhere(*ctx.oC_Where());
    }
    return withStatement;
}

unique_ptr<ReturnStatement> Transformer::transformReturn(CypherParser::OC_ReturnContext& ctx) {
    auto projectionItems = ctx.oC_ProjectionBody()->oC_ProjectionItems();
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    for (auto& projectionItem : projectionItems->oC_ProjectionItem()) {
        expressions.push_back(transformProjectionItem(*projectionItem));
    }
    return make_unique<ReturnStatement>(move(expressions), nullptr != projectionItems->STAR());
}

unique_ptr<ParsedExpression> Transformer::transformProjectionItem(
    CypherParser::OC_ProjectionItemContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    if (ctx.AS()) {
        expression->alias = transformVariable(*ctx.oC_Variable());
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformWhere(CypherParser::OC_WhereContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

vector<unique_ptr<PatternElement>> Transformer::transformPattern(
    CypherParser::OC_PatternContext& ctx) {
    vector<unique_ptr<PatternElement>> pattern;
    for (auto& patternPart : ctx.oC_PatternPart()) {
        pattern.push_back(transformPatternPart(*patternPart));
    }
    return pattern;
}

unique_ptr<PatternElement> Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext& ctx) {
    return transformAnonymousPatternPart(*ctx.oC_AnonymousPatternPart());
}

unique_ptr<PatternElement> Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext& ctx) {
    return transformPatternElement(*ctx.oC_PatternElement());
}

unique_ptr<PatternElement> Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext& ctx) {
    if (ctx.oC_PatternElement()) { // parenthesized pattern element
        return transformPatternElement(*ctx.oC_PatternElement());
    }
    auto patternElement = make_unique<PatternElement>(transformNodePattern(*ctx.oC_NodePattern()));
    if (!ctx.oC_PatternElementChain().empty()) {
        for (auto& patternElementChain : ctx.oC_PatternElementChain()) {
            patternElement->patternElementChains.push_back(
                transformPatternElementChain(*patternElementChain));
        }
    }
    return patternElement;
}

unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
    return make_unique<NodePattern>(
        ctx.oC_Variable() ? transformVariable(*ctx.oC_Variable()) : string(),
        ctx.oC_NodeLabel() ? transformNodeLabel(*ctx.oC_NodeLabel()) : string());
}

unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext& ctx) {
    return make_unique<PatternElementChain>(
        transformRelationshipPattern(*ctx.oC_RelationshipPattern()),
        transformNodePattern(*ctx.oC_NodePattern()));
}

unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
    auto relPattern = transformRelationshipDetail(*ctx.oC_RelationshipDetail());
    relPattern->arrowDirection =
        ctx.oC_LeftArrowHead() ? ArrowDirection::LEFT : ArrowDirection::RIGHT;
    return relPattern;
}

unique_ptr<RelPattern> Transformer::transformRelationshipDetail(
    CypherParser::OC_RelationshipDetailContext& ctx) {
    uint64_t lowerBound = 1u;
    uint64_t upperBound = 1u;
    if (ctx.oC_RangeLiteral()) {
        stringstream(ctx.oC_RangeLiteral()->oC_IntegerLiteral()[0]->getText()) >> lowerBound;
        stringstream(ctx.oC_RangeLiteral()->oC_IntegerLiteral()[1]->getText()) >> upperBound;
    }
    return make_unique<RelPattern>(
        ctx.oC_Variable() ? transformVariable(*ctx.oC_Variable()) : string(),
        ctx.oC_RelTypeName() ? transformRelTypeName(*ctx.oC_RelTypeName()) : string(), lowerBound,
        upperBound);
}

string Transformer::transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx) {
    return transformLabelName(*ctx.oC_LabelName());
}

string Transformer::transformLabelName(CypherParser::OC_LabelNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

string Transformer::transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformExpression(
    CypherParser::OC_ExpressionContext& ctx) {
    return transformOrExpression(*ctx.oC_OrExpression());
}

unique_ptr<ParsedExpression> Transformer::transformOrExpression(
    CypherParser::OC_OrExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto& xorExpression : ctx.oC_XorExpression()) {
        auto next = transformXorExpression(*xorExpression);
        if (!expression) {
            expression = move(next);
        } else {
            auto rawExpression = expression->rawExpression + " OR " + next->rawExpression;
            expression = make_unique<ParsedExpression>(
                OR, string(), rawExpression, move(expression), move(next));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformXorExpression(
    CypherParser::OC_XorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto& andExpression : ctx.oC_AndExpression()) {
        auto next = transformAndExpression(*andExpression);
        if (!expression) {
            expression = move(next);
        } else {
            auto rawExpression = expression->rawExpression + " XOR " + next->rawExpression;
            expression = make_unique<ParsedExpression>(
                XOR, string(), rawExpression, move(expression), move(next));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAndExpression(
    CypherParser::OC_AndExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto& notExpression : ctx.oC_NotExpression()) {
        auto next = transformNotExpression(*notExpression);
        if (!expression) {
            expression = move(next);
        } else {
            auto rawExpression = expression->rawExpression + " AND " + next->rawExpression;
            expression = make_unique<ParsedExpression>(
                AND, string(), rawExpression, move(expression), move(next));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext& ctx) {
    if (ctx.NOT()) {
        auto expression = make_unique<ParsedExpression>(NOT, string(), ctx.getText());
        expression->children.push_back(
            transformComparisonExpression(*ctx.oC_ComparisonExpression()));
        return expression;
    }
    return transformComparisonExpression(*ctx.oC_ComparisonExpression());
}

unique_ptr<ParsedExpression> Transformer::transformComparisonExpression(
    CypherParser::OC_ComparisonExpressionContext& ctx) {
    if (1 == ctx.oC_AddOrSubtractExpression().size()) {
        return transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(0));
    }
    unique_ptr<ParsedExpression> expression;
    auto comparisonOperator = ctx.gF_ComparisonOperator()->getText();
    if (comparisonOperator == "=") {
        expression = make_unique<ParsedExpression>(EQUALS, string(), ctx.getText());
    } else if (comparisonOperator == "<>") {
        expression = make_unique<ParsedExpression>(NOT_EQUALS, string(), ctx.getText());
    } else if (comparisonOperator == ">") {
        expression = make_unique<ParsedExpression>(GREATER_THAN, string(), ctx.getText());
    } else if (comparisonOperator == ">=") {
        expression = make_unique<ParsedExpression>(GREATER_THAN_EQUALS, string(), ctx.getText());
    } else if (comparisonOperator == "<") {
        expression = make_unique<ParsedExpression>(LESS_THAN, string(), ctx.getText());
    } else if (comparisonOperator == "<=") {
        expression = make_unique<ParsedExpression>(LESS_THAN_EQUALS, string(), ctx.getText());
    } else {
        throw invalid_argument("Unable to parse ComparisonExpressionContext.");
    }
    expression->children.push_back(
        transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(0)));
    expression->children.push_back(
        transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(1)));
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next =
            transformMultiplyDivideModuloExpression(*ctx.oC_MultiplyDivideModuloExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            auto arithmeticOperator = ctx.gF_AddOrSubtractOperator(i - 1)->getText();
            auto rawExpression =
                expression->rawExpression + " " + arithmeticOperator + " " + next->rawExpression;
            if (arithmeticOperator == "+") {
                expression = make_unique<ParsedExpression>(
                    ADD, string(), rawExpression, move(expression), move(next));
            } else if (arithmeticOperator == "-") {
                expression = make_unique<ParsedExpression>(
                    SUBTRACT, string(), rawExpression, move(expression), move(next));
            } else {
                throw invalid_argument("Unable to parse AddOrSubtractExpressionContext");
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformMultiplyDivideModuloExpression(
    CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_PowerOfExpression().size(); i++) {
        auto next = transformPowerOfExpression(*ctx.oC_PowerOfExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            auto arithmeticOperator = ctx.gF_MultiplyDivideModuloOperator(i - 1)->getText();
            auto rawExpression =
                expression->rawExpression + " " + arithmeticOperator + " " + next->rawExpression;
            if (arithmeticOperator == "*") {
                expression = make_unique<ParsedExpression>(
                    MULTIPLY, string(), rawExpression, move(expression), move(next));
            } else if (arithmeticOperator == "/") {
                expression = make_unique<ParsedExpression>(
                    DIVIDE, string(), rawExpression, move(expression), move(next));
            } else if (arithmeticOperator == "%") {
                expression = make_unique<ParsedExpression>(
                    MODULO, string(), rawExpression, move(expression), move(next));
            } else {
                throw invalid_argument("Unable to parse MultiplyDivideModuloExpressionContext.");
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto& unaryAddOrSubtractExpression : ctx.oC_UnaryAddOrSubtractExpression()) {
        auto next = transformUnaryAddOrSubtractExpression(*unaryAddOrSubtractExpression);
        if (!expression) {
            expression = move(next);
        } else {
            auto rawExpression = expression->rawExpression + " ^ " + next->rawExpression;
            expression = make_unique<ParsedExpression>(
                POWER, string(), rawExpression, move(expression), move(next));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformUnaryAddOrSubtractExpression(
    CypherParser::OC_UnaryAddOrSubtractExpressionContext& ctx) {
    if (ctx.MINUS()) {
        auto expression = make_unique<ParsedExpression>(NEGATE, string(), ctx.getText());
        expression->children.push_back(
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()));
        return expression;
    }
    return transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression());
}

unique_ptr<ParsedExpression> Transformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext& ctx) {
    auto propertyExpression =
        transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(
            *ctx.oC_NullOperatorExpression(), move(propertyExpression));
    }
    if (ctx.oC_ListOperatorExpression()) {
        return transformListOperatorExpression(
            *ctx.oC_ListOperatorExpression(), move(propertyExpression));
    }
    if (ctx.oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(
            *ctx.oC_StringOperatorExpression(), move(propertyExpression));
    }
    return propertyExpression;
}

unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    unique_ptr<ParsedExpression> expression;
    auto rawExpression = propertyExpression->rawExpression + " " + ctx.getText();
    if (ctx.STARTS()) {
        expression = make_unique<ParsedExpression>(STARTS_WITH, string(), rawExpression);
    } else if (ctx.ENDS()) {
        expression = make_unique<ParsedExpression>(ENDS_WITH, string(), rawExpression);
    } else if (ctx.CONTAINS()) {
        expression = make_unique<ParsedExpression>(CONTAINS, string(), rawExpression);
    } else {
        throw invalid_argument("Unable to parse StringOperatorExpressionContext.");
    }
    expression->children.push_back(move(propertyExpression));
    expression->children.push_back(
        transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression()));
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->rawExpression + " " + ctx.getText();
    auto expression = make_unique<ParsedExpression>(CSV_LINE_EXTRACT, string(), rawExpression);
    expression->children.push_back(move(propertyExpression));
    expression->children.push_back(transformExpression(*ctx.oC_Expression()));
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->rawExpression + " " + ctx.getText();
    if (ctx.IS() && ctx.NULL_()) {
        auto expression = ctx.NOT() ?
                              make_unique<ParsedExpression>(IS_NOT_NULL, string(), rawExpression) :
                              make_unique<ParsedExpression>(IS_NULL, string(), rawExpression);
        expression->children.push_back(move(propertyExpression));
        return expression;
    } else {
        throw invalid_argument("Unable to parse NullOperatorExpressionContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext& ctx) {
    if (ctx.oC_PropertyLookup()) {
        auto expression = make_unique<ParsedExpression>(
            PROPERTY, transformPropertyLookup(*ctx.oC_PropertyLookup()), ctx.getText());
        expression->children.push_back(transformAtom(*ctx.oC_Atom()));
        return expression;
    }
    return transformAtom(*ctx.oC_Atom());
}

unique_ptr<ParsedExpression> Transformer::transformAtom(CypherParser::OC_AtomContext& ctx) {
    if (ctx.oC_Literal()) {
        return transformLiteral(*ctx.oC_Literal());
    } else if (ctx.STAR()) {
        return make_unique<ParsedExpression>(FUNCTION, "COUNT_STAR", ctx.getText());
    } else if (ctx.oC_ParenthesizedExpression()) {
        return transformParenthesizedExpression(*ctx.oC_ParenthesizedExpression());
    } else if (ctx.oC_FunctionInvocation()) {
        return transformFunctionInvocation(*ctx.oC_FunctionInvocation());
    } else if (ctx.oC_Variable()) {
        return make_unique<ParsedExpression>(
            VARIABLE, transformVariable(*ctx.oC_Variable()), ctx.getText());
    } else {
        throw invalid_argument("Unable to parse AtomContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformLiteral(CypherParser::OC_LiteralContext& ctx) {
    if (ctx.oC_NumberLiteral()) {
        return transformNumberLiteral(*ctx.oC_NumberLiteral());
    } else if (ctx.oC_BooleanLiteral()) {
        return transformBooleanLiteral(*ctx.oC_BooleanLiteral());
    } else if (ctx.StringLiteral()) {
        return make_unique<ParsedExpression>(
            LITERAL_STRING, ctx.StringLiteral()->getText(), ctx.getText());
    } else if (ctx.NULL_()) {
        return make_unique<ParsedExpression>(LITERAL_NULL, string(), ctx.getText());
    } else {
        throw invalid_argument("Unable to parse LiteralContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext& ctx) {
    if (ctx.TRUE()) {
        return make_unique<ParsedExpression>(LITERAL_BOOLEAN, "true", ctx.getText());
    } else if (ctx.FALSE()) {
        return make_unique<ParsedExpression>(LITERAL_BOOLEAN, "false", ctx.getText());
    } else {
        throw invalid_argument("Unable to parse BooleanLiteralContext.");
    }
}

unique_ptr<ParsedExpression> Transformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

unique_ptr<ParsedExpression> Transformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext& ctx) {
    auto expression = make_unique<ParsedExpression>(
        FUNCTION, transformFunctionName(*ctx.oC_FunctionName()), ctx.getText());
    for (auto& childExpr : ctx.oC_Expression()) {
        expression->children.push_back(transformExpression(*childExpr));
    }
    return expression;
}

string Transformer::transformFunctionName(CypherParser::OC_FunctionNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string Transformer::transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

string Transformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext& ctx) {
    if (ctx.oC_IntegerLiteral()) {
        return transformIntegerLiteral(*ctx.oC_IntegerLiteral());
    } else if (ctx.oC_DoubleLiteral()) {
        return transformDoubleLiteral(*ctx.oC_DoubleLiteral());
    } else {
        throw invalid_argument("Unable to parse NumberLiteralContext.");
    }
}

string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(
    CypherParser::OC_IntegerLiteralContext& ctx) {
    return make_unique<ParsedExpression>(
        LITERAL_INT, ctx.DecimalInteger()->getText(), ctx.getText());
}

unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(
    CypherParser::OC_DoubleLiteralContext& ctx) {
    return make_unique<ParsedExpression>(
        LITERAL_DOUBLE, ctx.RegularDecimalReal()->getText(), ctx.getText());
}

string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    if (ctx.UnescapedSymbolicName()) {
        return ctx.UnescapedSymbolicName()->getText();
    } else if (ctx.EscapedSymbolicName()) {
        return ctx.EscapedSymbolicName()->getText();
    } else if (ctx.HexLetter()) {
        return ctx.HexLetter()->getText();
    } else {
        throw invalid_argument("Unable to parse SymbolicNameContext: " + ctx.getText());
    }
}

} // namespace parser
} // namespace graphflow
