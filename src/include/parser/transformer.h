#pragma once

#include "cypher_parser.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "parser/query/regular_query.h"

using namespace std;

namespace kuzu {
namespace parser {

struct ParsedCaseAlternative;
struct SetItem;

class Transformer {
public:
    explicit Transformer(CypherParser::OC_CypherContext& root) : root{root} {}

    unique_ptr<Statement> transform();

private:
    unique_ptr<RegularQuery> transformQuery(CypherParser::OC_QueryContext& ctx);

    unique_ptr<RegularQuery> transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);

    unique_ptr<SingleQuery> transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);

    unique_ptr<SingleQuery> transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext& ctx);

    unique_ptr<QueryPart> transformQueryPart(CypherParser::KU_QueryPartContext& ctx);

    unique_ptr<UpdatingClause> transformUpdatingClause(CypherParser::OC_UpdatingClauseContext& ctx);

    unique_ptr<ReadingClause> transformReadingClause(CypherParser::OC_ReadingClauseContext& ctx);

    unique_ptr<ReadingClause> transformMatch(CypherParser::OC_MatchContext& ctx);

    unique_ptr<ReadingClause> transformUnwind(CypherParser::OC_UnwindContext& ctx);

    unique_ptr<UpdatingClause> transformCreate(CypherParser::OC_CreateContext& ctx);

    unique_ptr<UpdatingClause> transformSet(CypherParser::OC_SetContext& ctx);

    pair<unique_ptr<ParsedExpression>, unique_ptr<ParsedExpression>> transformSetItem(
        CypherParser::OC_SetItemContext& ctx);

    unique_ptr<UpdatingClause> transformDelete(CypherParser::OC_DeleteContext& ctx);

    unique_ptr<WithClause> transformWith(CypherParser::OC_WithContext& ctx);

    unique_ptr<ReturnClause> transformReturn(CypherParser::OC_ReturnContext& ctx);

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

    vector<pair<string, unique_ptr<ParsedExpression>>> transformProperties(
        CypherParser::KU_PropertiesContext& ctx);

    vector<string> transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx);

    vector<string> transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx);

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

    unique_ptr<ParsedExpression> transformBitwiseOrOperatorExpression(
        CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformBitwiseAndOperatorExpression(
        CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformBitShiftOperatorExpression(
        CypherParser::KU_BitShiftOperatorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformAddOrSubtractExpression(
        CypherParser::OC_AddOrSubtractExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(
        CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformPowerOfExpression(
        CypherParser::OC_PowerOfExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformUnaryAddSubtractOrFactorialExpression(
        CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(
        CypherParser::OC_StringListNullOperatorExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformStringOperatorExpression(
        CypherParser::OC_StringOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformListOperatorExpression(
        CypherParser::OC_ListOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformListSliceOperatorExpression(
        CypherParser::KU_ListSliceOperatorExpressionContext& ctx,
        unique_ptr<ParsedExpression> propertyExpression);

    unique_ptr<ParsedExpression> transformListExtractOperatorExpression(
        CypherParser::KU_ListExtractOperatorExpressionContext& ctx,
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

    unique_ptr<ParsedExpression> transformListLiteral(CypherParser::OC_ListLiteralContext& ctx);

    unique_ptr<ParsedExpression> transformParameterExpression(
        CypherParser::OC_ParameterContext& ctx);

    unique_ptr<ParsedExpression> transformParenthesizedExpression(
        CypherParser::OC_ParenthesizedExpressionContext& ctx);

    unique_ptr<ParsedExpression> transformFunctionInvocation(
        CypherParser::OC_FunctionInvocationContext& ctx);

    string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);

    unique_ptr<ParsedExpression> transformExistentialSubquery(
        CypherParser::OC_ExistentialSubqueryContext& ctx);

    string transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx);

    unique_ptr<ParsedExpression> transformCaseExpression(
        CypherParser::OC_CaseExpressionContext& ctx);

    unique_ptr<ParsedCaseAlternative> transformCaseAlternative(
        CypherParser::OC_CaseAlternativeContext& ctx);

    string transformVariable(CypherParser::OC_VariableContext& ctx);

    unique_ptr<ParsedExpression> transformNumberLiteral(CypherParser::OC_NumberLiteralContext& ctx);

    unique_ptr<ParsedExpression> transformProperty(CypherParser::OC_PropertyExpressionContext& ctx);

    string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);

    unique_ptr<ParsedExpression> transformIntegerLiteral(
        CypherParser::OC_IntegerLiteralContext& ctx);

    unique_ptr<ParsedExpression> transformDoubleLiteral(CypherParser::OC_DoubleLiteralContext& ctx);

    string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);

    string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);

    unique_ptr<Statement> transformDDL(CypherParser::KU_DDLContext& ctx);

    unique_ptr<Statement> transformCreateNodeClause(CypherParser::KU_CreateNodeContext& ctx);

    unique_ptr<Statement> transformCreateRelClause(CypherParser::KU_CreateRelContext& ctx);

    unique_ptr<Statement> transformDropTable(CypherParser::KU_DropTableContext& ctx);

    unique_ptr<Statement> transformAlterTable(CypherParser::KU_AlterTableContext& ctx);

    unique_ptr<Statement> transformAddProperty(CypherParser::KU_AlterTableContext& ctx);

    unique_ptr<Statement> transformDropProperty(CypherParser::KU_AlterTableContext& ctx);

    string transformDataType(CypherParser::KU_DataTypeContext& ctx);

    string transformListIdentifiers(CypherParser::KU_ListIdentifiersContext& ctx);

    string transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx);

    vector<pair<string, string>> transformPropertyDefinitions(
        CypherParser::KU_PropertyDefinitionsContext& ctx);

    vector<pair<string, string>> transformRelConnections(
        CypherParser::KU_RelConnectionsContext& ctx);

    vector<string> transformNodeLabels(CypherParser::KU_NodeLabelsContext& ctx);

    unique_ptr<Statement> transformCopyCSV(CypherParser::KU_CopyCSVContext& ctx);

    unordered_map<string, unique_ptr<ParsedExpression>> transformParsingOptions(
        CypherParser::KU_ParsingOptionsContext& ctx);

    string transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral);

private:
    CypherParser::OC_CypherContext& root;
};

} // namespace parser
} // namespace kuzu
