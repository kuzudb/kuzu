#pragma once

#include "cypher_parser.h"
#include "expression/parsed_expression.h"
#include "statement.h"

namespace kuzu {
namespace parser {

class RegularQuery;
class SingleQuery;
class QueryPart;
class UpdatingClause;
class ReadingClause;
class WithClause;
class ReturnClause;
class ProjectionBody;
class PatternElement;
class NodePattern;
class PatternElementChain;
class RelPattern;
struct ParsedCaseAlternative;

class Transformer {
public:
    explicit Transformer(CypherParser::OC_CypherContext& root) : root{root} {}

    std::unique_ptr<Statement> transform();

private:
    std::unique_ptr<Statement> transformStatement(CypherParser::OC_StatementContext& ctx);

    /* Copy From */
    std::unique_ptr<Statement> transformCopyTo(CypherParser::KU_CopyTOContext& ctx);
    std::unique_ptr<Statement> transformCopyFrom(CypherParser::KU_CopyFromContext& ctx);
    std::unique_ptr<Statement> transformCopyFromByColumn(
        CypherParser::KU_CopyFromByColumnContext& ctx);
    std::vector<std::string> transformColumnNames(CypherParser::KU_ColumnNamesContext& ctx);
    std::vector<std::string> transformFilePaths(
        std::vector<antlr4::tree::TerminalNode*> stringLiteral);
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> transformParsingOptions(
        CypherParser::KU_ParsingOptionsContext& ctx);

    std::unique_ptr<RegularQuery> transformQuery(CypherParser::OC_QueryContext& ctx);

    std::unique_ptr<RegularQuery> transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);

    std::unique_ptr<SingleQuery> transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);

    std::unique_ptr<SingleQuery> transformSinglePartQuery(
        CypherParser::OC_SinglePartQueryContext& ctx);

    std::unique_ptr<QueryPart> transformQueryPart(CypherParser::KU_QueryPartContext& ctx);

    std::unique_ptr<UpdatingClause> transformUpdatingClause(
        CypherParser::OC_UpdatingClauseContext& ctx);

    std::unique_ptr<ReadingClause> transformReadingClause(
        CypherParser::OC_ReadingClauseContext& ctx);

    std::unique_ptr<ReadingClause> transformMatch(CypherParser::OC_MatchContext& ctx);
    std::unique_ptr<ReadingClause> transformUnwind(CypherParser::OC_UnwindContext& ctx);
    std::unique_ptr<ReadingClause> transformInQueryCall(CypherParser::KU_InQueryCallContext& ctx);
    std::unique_ptr<ReadingClause> transformLoadFrom(CypherParser::KU_LoadFromContext& ctx);

    std::unique_ptr<UpdatingClause> transformCreate(CypherParser::OC_CreateContext& ctx);

    std::unique_ptr<UpdatingClause> transformMerge(CypherParser::OC_MergeContext& ctx);

    std::unique_ptr<UpdatingClause> transformSet(CypherParser::OC_SetContext& ctx);

    parsed_expression_pair transformSetItem(CypherParser::OC_SetItemContext& ctx);

    std::unique_ptr<UpdatingClause> transformDelete(CypherParser::OC_DeleteContext& ctx);

    std::unique_ptr<WithClause> transformWith(CypherParser::OC_WithContext& ctx);

    std::unique_ptr<ReturnClause> transformReturn(CypherParser::OC_ReturnContext& ctx);

    std::unique_ptr<ProjectionBody> transformProjectionBody(
        CypherParser::OC_ProjectionBodyContext& ctx);

    std::vector<std::unique_ptr<ParsedExpression>> transformProjectionItems(
        CypherParser::OC_ProjectionItemsContext& ctx);

    std::unique_ptr<ParsedExpression> transformProjectionItem(
        CypherParser::OC_ProjectionItemContext& ctx);

    std::unique_ptr<ParsedExpression> transformWhere(CypherParser::OC_WhereContext& ctx);

    std::vector<std::unique_ptr<PatternElement>> transformPattern(
        CypherParser::OC_PatternContext& ctx);

    std::unique_ptr<PatternElement> transformPatternPart(CypherParser::OC_PatternPartContext& ctx);

    std::unique_ptr<PatternElement> transformAnonymousPatternPart(
        CypherParser::OC_AnonymousPatternPartContext& ctx);

    std::unique_ptr<PatternElement> transformPatternElement(
        CypherParser::OC_PatternElementContext& ctx);

    std::unique_ptr<NodePattern> transformNodePattern(CypherParser::OC_NodePatternContext& ctx);

    std::unique_ptr<PatternElementChain> transformPatternElementChain(
        CypherParser::OC_PatternElementChainContext& ctx);

    std::unique_ptr<RelPattern> transformRelationshipPattern(
        CypherParser::OC_RelationshipPatternContext& ctx);

    std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> transformProperties(
        CypherParser::KU_PropertiesContext& ctx);

    std::vector<std::string> transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx);

    std::vector<std::string> transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx);

    std::string transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx);

    std::string transformLabelName(CypherParser::OC_LabelNameContext& ctx);

    std::string transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx);

    std::unique_ptr<ParsedExpression> transformExpression(CypherParser::OC_ExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformOrExpression(
        CypherParser::OC_OrExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformXorExpression(
        CypherParser::OC_XorExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformAndExpression(
        CypherParser::OC_AndExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformNotExpression(
        CypherParser::OC_NotExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformComparisonExpression(
        CypherParser::OC_ComparisonExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformBitwiseOrOperatorExpression(
        CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformBitwiseAndOperatorExpression(
        CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformBitShiftOperatorExpression(
        CypherParser::KU_BitShiftOperatorExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformAddOrSubtractExpression(
        CypherParser::OC_AddOrSubtractExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(
        CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformPowerOfExpression(
        CypherParser::OC_PowerOfExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformUnaryAddSubtractOrFactorialExpression(
        CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(
        CypherParser::OC_StringListNullOperatorExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformStringOperatorExpression(
        CypherParser::OC_StringOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);

    std::unique_ptr<ParsedExpression> transformListOperatorExpression(
        CypherParser::OC_ListOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> childExpression);

    std::unique_ptr<ParsedExpression> transformListSliceOperatorExpression(
        CypherParser::KU_ListSliceOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);

    std::unique_ptr<ParsedExpression> transformListExtractOperatorExpression(
        CypherParser::KU_ListExtractOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);

    std::unique_ptr<ParsedExpression> transformNullOperatorExpression(
        CypherParser::OC_NullOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);

    std::unique_ptr<ParsedExpression> transformPropertyOrLabelsExpression(
        CypherParser::OC_PropertyOrLabelsExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformAtom(CypherParser::OC_AtomContext& ctx);

    std::unique_ptr<ParsedExpression> transformLiteral(CypherParser::OC_LiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformBooleanLiteral(
        CypherParser::OC_BooleanLiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformListLiteral(
        CypherParser::OC_ListLiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformStructLiteral(
        CypherParser::KU_StructLiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformParameterExpression(
        CypherParser::OC_ParameterContext& ctx);

    std::unique_ptr<ParsedExpression> transformParenthesizedExpression(
        CypherParser::OC_ParenthesizedExpressionContext& ctx);

    std::unique_ptr<ParsedExpression> transformFunctionInvocation(
        CypherParser::OC_FunctionInvocationContext& ctx);

    std::string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);

    std::unique_ptr<ParsedExpression> transformFunctionParameterExpression(
        CypherParser::KU_FunctionParameterContext& ctx);

    std::unique_ptr<ParsedExpression> transformPathPattern(
        CypherParser::OC_PathPatternsContext& ctx);
    std::unique_ptr<ParsedExpression> transformExistSubquery(
        CypherParser::OC_ExistSubqueryContext& ctx);
    std::unique_ptr<ParsedExpression> transformCountSubquery(
        CypherParser::KU_CountSubqueryContext& ctx);

    std::unique_ptr<ParsedExpression> createPropertyExpression(
        CypherParser::OC_PropertyLookupContext& ctx, std::unique_ptr<ParsedExpression> child);

    std::unique_ptr<ParsedExpression> transformCaseExpression(
        CypherParser::OC_CaseExpressionContext& ctx);

    std::unique_ptr<ParsedCaseAlternative> transformCaseAlternative(
        CypherParser::OC_CaseAlternativeContext& ctx);

    std::string transformVariable(CypherParser::OC_VariableContext& ctx);

    std::unique_ptr<ParsedExpression> transformNumberLiteral(
        CypherParser::OC_NumberLiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformProperty(
        CypherParser::OC_PropertyExpressionContext& ctx);

    std::string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);

    std::unique_ptr<ParsedExpression> transformIntegerLiteral(
        CypherParser::OC_IntegerLiteralContext& ctx);

    std::unique_ptr<ParsedExpression> transformDoubleLiteral(
        CypherParser::OC_DoubleLiteralContext& ctx);

    std::string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);

    std::string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);

    std::unique_ptr<Statement> transformDDL(CypherParser::KU_DDLContext& ctx);

    std::unique_ptr<Statement> transformAlterTable(CypherParser::KU_AlterTableContext& ctx);

    std::unique_ptr<Statement> transformCreateNodeTable(
        CypherParser::KU_CreateNodeTableContext& ctx);

    std::unique_ptr<Statement> transformCreateRelTable(CypherParser::KU_CreateRelTableContext& ctx);

    std::unique_ptr<Statement> transformCreateRelTableGroup(
        CypherParser::KU_CreateRelTableGroupContext& ctx);

    std::unique_ptr<Statement> transformCreateRdfGraphClause(
        CypherParser::KU_CreateRdfGraphContext& ctx);

    std::unique_ptr<Statement> transformDropTable(CypherParser::KU_DropTableContext& ctx);

    std::unique_ptr<Statement> transformRenameTable(CypherParser::KU_AlterTableContext& ctx);

    std::unique_ptr<Statement> transformAddProperty(CypherParser::KU_AlterTableContext& ctx);

    std::unique_ptr<Statement> transformDropProperty(CypherParser::KU_AlterTableContext& ctx);

    std::unique_ptr<Statement> transformRenameProperty(CypherParser::KU_AlterTableContext& ctx);

    std::string transformDataType(CypherParser::KU_DataTypeContext& ctx);

    std::string transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx);

    std::vector<std::pair<std::string, std::string>> transformPropertyDefinitions(
        CypherParser::KU_PropertyDefinitionsContext& ctx);

    std::unique_ptr<Statement> transformStandaloneCall(CypherParser::KU_StandaloneCallContext& ctx);

    std::vector<std::string> transformPositionalArgs(CypherParser::KU_PositionalArgsContext& ctx);

    std::unique_ptr<Statement> transformCreateMacro(CypherParser::KU_CreateMacroContext& ctx);

    std::unique_ptr<Statement> transformTransaction(CypherParser::KU_TransactionContext& ctx);

    std::unique_ptr<Statement> transformCommentOn(CypherParser::KU_CommentOnContext& ctx);

    std::string transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral);

private:
    CypherParser::OC_CypherContext& root;
};

} // namespace parser
} // namespace kuzu
