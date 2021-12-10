#pragma once

#include "src/antlr4/CypherParser.h"
#include "src/parser/include/queries/single_query.h"

using namespace std;

namespace graphflow {
namespace parser {

class Transformer {
public:
    explicit Transformer(CypherParser::OC_CypherContext& root) : root{root} {}

    unique_ptr<SingleQuery> transform();

private:
    unique_ptr<SingleQuery> transformQuery(CypherParser::OC_QueryContext& ctx);

    unique_ptr<SingleQuery> transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);

    unique_ptr<SingleQuery> transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);

    unique_ptr<SingleQuery> transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext& ctx);

    unique_ptr<QueryPart> transformQueryPart(CypherParser::GF_QueryPartContext& ctx);

    unique_ptr<MatchStatement> transformReadingClause(CypherParser::OC_ReadingClauseContext& ctx);

    unique_ptr<MatchStatement> transformMatch(CypherParser::OC_MatchContext& ctx);

    unique_ptr<WithStatement> transformWith(CypherParser::OC_WithContext& ctx);

    unique_ptr<ReturnStatement> transformReturn(CypherParser::OC_ReturnContext& ctx);

    unique_ptr<ProjectionBody> transformProjectionBody(CypherParser::OC_ProjectionBodyContext& ctx);

    vector<unique_ptr<ParsedExpression>> transformProjectionItems(
        CypherParser::OC_ProjectionItemsContext& ctx);

    unique_ptr<ParsedExpression> transformProjectionItem(
        CypherParser::OC_ProjectionItemContext& ctx);

    unique_ptr<ParsedExpression> transformWhere(CypherParser::OC_WhereContext& ctx);

    vector<unique_ptr<PatternElement>> transformPattern(CypherParser::OC_PatternContext& ctx);

    unique_ptr<PatternElement> transformPatternPart(CypherParser::OC_PatternPartContext& ctx);

    unique_ptr<PatternElement> transformAnonymousPatternPart(
        CypherParser::OC_AnonymousPatternPartContext& ctx);

    unique_ptr<PatternElement> transformPatternElement(CypherParser::OC_PatternElementContext& ctx);

    unique_ptr<NodePattern> transformNodePattern(CypherParser::OC_NodePatternContext& ctx);

    unique_ptr<PatternElementChain> transformPatternElementChain(
        CypherParser::OC_PatternElementChainContext& ctx);

    unique_ptr<RelPattern> transformRelationshipPattern(
        CypherParser::OC_RelationshipPatternContext& ctx);

    unique_ptr<RelPattern> transformRelationshipDetail(
        CypherParser::OC_RelationshipDetailContext& ctx);

    string transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx);

    string transformLabelName(CypherParser::OC_LabelNameContext& ctx);

    string transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx);

    unique_ptr<ParsedExpression> transformExpression(CypherParser::OC_ExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformOrExpression(CypherParser::OC_OrExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformXorExpression(CypherParser::OC_XorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformAndExpression(CypherParser::OC_AndExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformNotExpression(CypherParser::OC_NotExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformComparisonExpression(
        CypherParser::OC_ComparisonExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformAddOrSubtractExpression(
        CypherParser::OC_AddOrSubtractExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(
        CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformPowerOfExpression(
        CypherParser::OC_PowerOfExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformUnaryAddOrSubtractExpression(
        CypherParser::OC_UnaryAddOrSubtractExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(
        CypherParser::OC_StringListNullOperatorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformStringOperatorExpression(
        CypherParser::OC_StringOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformListOperatorExpression(
        CypherParser::OC_ListOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformNullOperatorExpression(
        CypherParser::OC_NullOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformPropertyOrLabelsExpression(
        CypherParser::OC_PropertyOrLabelsExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformAtom(CypherParser::OC_AtomContext& ctx);

    unique_ptr<ParsedExpression> transformLiteral(CypherParser::OC_LiteralContext& ctx);

    unique_ptr<ParsedExpression> transformBooleanLiteral(
        CypherParser::OC_BooleanLiteralContext& ctx);

    unique_ptr<ParsedExpression> transformParenthesizedExpression(
        CypherParser::OC_ParenthesizedExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformFunctionInvocation(
        CypherParser::OC_FunctionInvocationContext& ctx);

    string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);

    unique_ptr<ParsedExpression> transformExistentialSubquery(
        CypherParser::OC_ExistentialSubqueryContext& ctx);

    string transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx);

    string transformVariable(CypherParser::OC_VariableContext& ctx);

    unique_ptr<ParsedExpression> transformNumberLiteral(CypherParser::OC_NumberLiteralContext& ctx);

    string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);

    unique_ptr<ParsedExpression> transformIntegerLiteral(
        CypherParser::OC_IntegerLiteralContext& ctx);

    unique_ptr<ParsedExpression> transformDoubleLiteral(CypherParser::OC_DoubleLiteralContext& ctx);

    string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);

    string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);

private:
    CypherParser::OC_CypherContext& root;
};

} // namespace parser
} // namespace graphflow
