#pragma once

// ANTLR4 generates code with unused parameters.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_parser.h"
#pragma GCC diagnostic pop

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

    std::unique_ptr<ParsedExpression> transformWhere(CypherParser::OC_WhereContext& ctx);

    std::string transformVariable(CypherParser::OC_VariableContext& ctx);
    std::string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);
    std::string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);
    std::string transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral);

    // Transform copy statement.
    std::unique_ptr<Statement> transformCopyTo(CypherParser::KU_CopyTOContext& ctx);
    std::unique_ptr<Statement> transformCopyFrom(CypherParser::KU_CopyFromContext& ctx);
    std::unique_ptr<Statement> transformCopyFromByColumn(
        CypherParser::KU_CopyFromByColumnContext& ctx);
    std::vector<std::string> transformColumnNames(CypherParser::KU_ColumnNamesContext& ctx);
    std::vector<std::string> transformFilePaths(
        const std::vector<antlr4::tree::TerminalNode*>& stringLiteral);
    parsing_option_t transformParsingOptions(CypherParser::KU_ParsingOptionsContext& ctx);

    std::unique_ptr<Statement> transformExportDatabase(CypherParser::KU_ExportDatabaseContext& ctx);

    // Transform query statement.
    std::unique_ptr<Statement> transformQuery(CypherParser::OC_QueryContext& ctx);
    std::unique_ptr<Statement> transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);
    SingleQuery transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);
    SingleQuery transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext& ctx);
    QueryPart transformQueryPart(CypherParser::KU_QueryPartContext& ctx);

    // Transform updating.
    std::unique_ptr<UpdatingClause> transformUpdatingClause(
        CypherParser::OC_UpdatingClauseContext& ctx);
    std::unique_ptr<UpdatingClause> transformCreate(CypherParser::OC_CreateContext& ctx);
    std::unique_ptr<UpdatingClause> transformMerge(CypherParser::OC_MergeContext& ctx);
    std::unique_ptr<UpdatingClause> transformSet(CypherParser::OC_SetContext& ctx);
    parsed_expr_pair transformSetItem(CypherParser::OC_SetItemContext& ctx);
    std::unique_ptr<UpdatingClause> transformDelete(CypherParser::OC_DeleteContext& ctx);

    // Transform reading.
    std::unique_ptr<ReadingClause> transformReadingClause(
        CypherParser::OC_ReadingClauseContext& ctx);
    std::unique_ptr<ReadingClause> transformMatch(CypherParser::OC_MatchContext& ctx);
    std::unique_ptr<ReadingClause> transformUnwind(CypherParser::OC_UnwindContext& ctx);
    std::unique_ptr<ReadingClause> transformInQueryCall(CypherParser::KU_InQueryCallContext& ctx);
    std::unique_ptr<ReadingClause> transformLoadFrom(CypherParser::KU_LoadFromContext& ctx);

    // Transform projection.
    WithClause transformWith(CypherParser::OC_WithContext& ctx);
    ReturnClause transformReturn(CypherParser::OC_ReturnContext& ctx);
    ProjectionBody transformProjectionBody(CypherParser::OC_ProjectionBodyContext& ctx);
    std::vector<std::unique_ptr<ParsedExpression>> transformProjectionItems(
        CypherParser::OC_ProjectionItemsContext& ctx);
    std::unique_ptr<ParsedExpression> transformProjectionItem(
        CypherParser::OC_ProjectionItemContext& ctx);

    // Transform graph pattern.
    std::vector<PatternElement> transformPattern(CypherParser::OC_PatternContext& ctx);
    PatternElement transformPatternPart(CypherParser::OC_PatternPartContext& ctx);
    PatternElement transformAnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext& ctx);
    PatternElement transformPatternElement(CypherParser::OC_PatternElementContext& ctx);
    NodePattern transformNodePattern(CypherParser::OC_NodePatternContext& ctx);
    PatternElementChain transformPatternElementChain(
        CypherParser::OC_PatternElementChainContext& ctx);
    RelPattern transformRelationshipPattern(CypherParser::OC_RelationshipPatternContext& ctx);
    std::vector<s_parsed_expr_pair> transformProperties(CypherParser::KU_PropertiesContext& ctx);
    std::vector<std::string> transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx);
    std::vector<std::string> transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx);
    std::string transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx);
    std::string transformLabelName(CypherParser::OC_LabelNameContext& ctx);
    std::string transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx);

    // Transform expression.
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
    ParsedCaseAlternative transformCaseAlternative(CypherParser::OC_CaseAlternativeContext& ctx);
    std::unique_ptr<ParsedExpression> transformNumberLiteral(
        CypherParser::OC_NumberLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformProperty(
        CypherParser::OC_PropertyExpressionContext& ctx);
    std::string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);
    std::unique_ptr<ParsedExpression> transformIntegerLiteral(
        CypherParser::OC_IntegerLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformDoubleLiteral(
        CypherParser::OC_DoubleLiteralContext& ctx);

    // Transform ddl.
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

    // Transform standalone call.
    std::unique_ptr<Statement> transformStandaloneCall(CypherParser::KU_StandaloneCallContext& ctx);

    // Transform create macro.
    std::unique_ptr<Statement> transformCreateMacro(CypherParser::KU_CreateMacroContext& ctx);
    std::vector<std::string> transformPositionalArgs(CypherParser::KU_PositionalArgsContext& ctx);

    // Transform transaction.
    std::unique_ptr<Statement> transformTransaction(CypherParser::KU_TransactionContext& ctx);

    // Transform extension.
    std::unique_ptr<Statement> transformExtension(CypherParser::KU_ExtensionContext& ctx);

    // Transform comment on.
    std::unique_ptr<Statement> transformCommentOn(CypherParser::KU_CommentOnContext& ctx);

private:
    CypherParser::OC_CypherContext& root;
};

} // namespace parser
} // namespace kuzu
