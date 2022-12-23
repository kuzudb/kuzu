#include "parser/transformer.h"

#include "common/exception.h"
#include "common/utils.h"
#include "parser/copy_csv/copy_csv.h"
#include "parser/ddl/create_node_clause.h"
#include "parser/ddl/create_rel_clause.h"
#include "parser/ddl/drop_table.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/expression/parsed_parameter_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_subquery_expression.h"
#include "parser/expression/parsed_variable_expression.h"
#include "parser/query/reading_clause/unwind_clause.h"
#include "parser/query/updating_clause/create_clause.h"
#include "parser/query/updating_clause/delete_clause.h"
#include "parser/query/updating_clause/set_clause.h"

using namespace std;

namespace kuzu {
namespace parser {

unique_ptr<Statement> Transformer::transform() {
    if (root.oC_Statement()) {
        return transformQuery();
    } else if (root.kU_DDL()) {
        return transformDDL();
    } else {
        return transformCopyCSV();
    }
}

unique_ptr<RegularQuery> Transformer::transformQuery() {
    auto regularQuery = transformQuery(*root.oC_Statement()->oC_Query());
    if (root.oC_AnyCypherOption()) {
        auto cypherOption = root.oC_AnyCypherOption();
        regularQuery->setEnableExplain(cypherOption->oC_Explain() != nullptr);
        regularQuery->setEnableProfile(cypherOption->oC_Profile() != nullptr);
    }
    return regularQuery;
}

unique_ptr<RegularQuery> Transformer::transformQuery(CypherParser::OC_QueryContext& ctx) {
    return transformRegularQuery(*ctx.oC_RegularQuery());
}

unique_ptr<RegularQuery> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
    auto regularQuery = make_unique<RegularQuery>(transformSingleQuery(*ctx.oC_SingleQuery()));
    for (auto unionClause : ctx.oC_Union()) {
        regularQuery->addSingleQuery(
            transformSingleQuery(*unionClause->oC_SingleQuery()), unionClause->ALL());
    }
    return regularQuery;
}

unique_ptr<SingleQuery> Transformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext& ctx) {
    auto singleQuery =
        ctx.oC_MultiPartQuery() ?
            transformSinglePartQuery(*ctx.oC_MultiPartQuery()->oC_SinglePartQuery()) :
            transformSinglePartQuery(*ctx.oC_SinglePartQuery());
    if (ctx.oC_MultiPartQuery()) {
        for (auto queryPart : ctx.oC_MultiPartQuery()->kU_QueryPart()) {
            singleQuery->addQueryPart(transformQueryPart(*queryPart));
        }
    }
    return singleQuery;
}

unique_ptr<SingleQuery> Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext& ctx) {
    auto singleQuery = make_unique<SingleQuery>();
    for (auto& readingClause : ctx.oC_ReadingClause()) {
        singleQuery->addReadingClause(transformReadingClause(*readingClause));
    }
    for (auto& updatingClause : ctx.oC_UpdatingClause()) {
        singleQuery->addUpdatingClause(transformUpdatingClause(*updatingClause));
    }
    if (ctx.oC_Return()) {
        singleQuery->setReturnClause(transformReturn(*ctx.oC_Return()));
    }
    return singleQuery;
}

unique_ptr<QueryPart> Transformer::transformQueryPart(CypherParser::KU_QueryPartContext& ctx) {
    auto queryPart = make_unique<QueryPart>(transformWith(*ctx.oC_With()));
    for (auto& readingClause : ctx.oC_ReadingClause()) {
        queryPart->addReadingClause(transformReadingClause(*readingClause));
    }
    for (auto& updatingClause : ctx.oC_UpdatingClause()) {
        queryPart->addUpdatingClause(transformUpdatingClause(*updatingClause));
    }
    return queryPart;
}

unique_ptr<UpdatingClause> Transformer::transformUpdatingClause(
    CypherParser::OC_UpdatingClauseContext& ctx) {
    if (ctx.oC_Create()) {
        return transformCreate(*ctx.oC_Create());
    } else if (ctx.oC_Set()) {
        return transformSet(*ctx.oC_Set());
    } else {
        assert(ctx.oC_Delete());
        return transformDelete(*ctx.oC_Delete());
    }
}

unique_ptr<ReadingClause> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    if (ctx.oC_Match()) {
        return transformMatch(*ctx.oC_Match());
    } else {
        assert(ctx.oC_Unwind());
        return transformUnwind(*ctx.oC_Unwind());
    }
}

unique_ptr<ReadingClause> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchClause =
        make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()), ctx.OPTIONAL());
    if (ctx.oC_Where()) {
        matchClause->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return matchClause;
}

unique_ptr<ReadingClause> Transformer::transformUnwind(CypherParser::OC_UnwindContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    auto transformedVariable = transformVariable(*ctx.oC_Variable());
    return make_unique<UnwindClause>(std::move(expression), std::move(transformedVariable));
}

unique_ptr<UpdatingClause> Transformer::transformCreate(CypherParser::OC_CreateContext& ctx) {
    return make_unique<CreateClause>(transformPattern(*ctx.oC_Pattern()));
}

unique_ptr<UpdatingClause> Transformer::transformSet(CypherParser::OC_SetContext& ctx) {
    auto setClause = make_unique<SetClause>();
    for (auto& setItem : ctx.oC_SetItem()) {
        setClause->addSetItem(transformSetItem(*setItem));
    }
    return setClause;
}

pair<unique_ptr<ParsedExpression>, unique_ptr<ParsedExpression>> Transformer::transformSetItem(
    CypherParser::OC_SetItemContext& ctx) {
    return make_pair(
        transformProperty(*ctx.oC_PropertyExpression()), transformExpression(*ctx.oC_Expression()));
}

unique_ptr<UpdatingClause> Transformer::transformDelete(CypherParser::OC_DeleteContext& ctx) {
    auto deleteClause = make_unique<DeleteClause>();
    for (auto& expression : ctx.oC_Expression()) {
        deleteClause->addExpression(transformExpression(*expression));
    }
    return deleteClause;
}

unique_ptr<WithClause> Transformer::transformWith(CypherParser::OC_WithContext& ctx) {
    auto withClause = make_unique<WithClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
    if (ctx.oC_Where()) {
        withClause->setWhereExpression(transformWhere(*ctx.oC_Where()));
    }
    return withClause;
}

unique_ptr<ReturnClause> Transformer::transformReturn(CypherParser::OC_ReturnContext& ctx) {
    return make_unique<ReturnClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
}

unique_ptr<ProjectionBody> Transformer::transformProjectionBody(
    CypherParser::OC_ProjectionBodyContext& ctx) {
    auto projectionBody = make_unique<ProjectionBody>(nullptr != ctx.DISTINCT(),
        nullptr != ctx.oC_ProjectionItems()->STAR(),
        transformProjectionItems(*ctx.oC_ProjectionItems()));
    if (ctx.oC_Order()) {
        vector<unique_ptr<ParsedExpression>> orderByExpressions;
        vector<bool> isAscOrders;
        for (auto& sortItem : ctx.oC_Order()->oC_SortItem()) {
            orderByExpressions.push_back(transformExpression(*sortItem->oC_Expression()));
            isAscOrders.push_back(!(sortItem->DESC() || sortItem->DESCENDING()));
        }
        projectionBody->setOrderByExpressions(
            std::move(orderByExpressions), std::move(isAscOrders));
    }
    if (ctx.oC_Skip()) {
        projectionBody->setSkipExpression(transformExpression(*ctx.oC_Skip()->oC_Expression()));
    }
    if (ctx.oC_Limit()) {
        projectionBody->setLimitExpression(transformExpression(*ctx.oC_Limit()->oC_Expression()));
    }
    return projectionBody;
}

vector<unique_ptr<ParsedExpression>> Transformer::transformProjectionItems(
    CypherParser::OC_ProjectionItemsContext& ctx) {
    vector<unique_ptr<ParsedExpression>> projectionExpressions;
    for (auto& projectionItem : ctx.oC_ProjectionItem()) {
        projectionExpressions.push_back(transformProjectionItem(*projectionItem));
    }
    return projectionExpressions;
}

unique_ptr<ParsedExpression> Transformer::transformProjectionItem(
    CypherParser::OC_ProjectionItemContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    if (ctx.AS()) {
        expression->setAlias(transformVariable(*ctx.oC_Variable()));
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
            patternElement->addPatternElementChain(
                transformPatternElementChain(*patternElementChain));
        }
    }
    return patternElement;
}

unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
    auto variable = ctx.oC_Variable() ? transformVariable(*ctx.oC_Variable()) : string();
    auto nodeLabels =
        ctx.oC_NodeLabels() ? transformNodeLabels(*ctx.oC_NodeLabels()) : vector<string>{};
    auto properties = ctx.kU_Properties() ? transformProperties(*ctx.kU_Properties()) :
                                            vector<pair<string, unique_ptr<ParsedExpression>>>{};
    return make_unique<NodePattern>(
        std::move(variable), std::move(nodeLabels), std::move(properties));
}

unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext& ctx) {
    return make_unique<PatternElementChain>(
        transformRelationshipPattern(*ctx.oC_RelationshipPattern()),
        transformNodePattern(*ctx.oC_NodePattern()));
}

unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
    auto relDetail = ctx.oC_RelationshipDetail();
    auto variable =
        relDetail->oC_Variable() ? transformVariable(*relDetail->oC_Variable()) : string();
    auto relTypes = relDetail->oC_RelationshipTypes() ?
                        transformRelTypes(*relDetail->oC_RelationshipTypes()) :
                        vector<string>{};
    string lowerBound = "1";
    string upperBound = "1";
    if (relDetail->oC_RangeLiteral()) {
        lowerBound = relDetail->oC_RangeLiteral()->oC_IntegerLiteral()[0]->getText();
        upperBound = relDetail->oC_RangeLiteral()->oC_IntegerLiteral()[1]->getText();
    }
    auto arrowHead = ctx.oC_LeftArrowHead() ? ArrowDirection::LEFT : ArrowDirection::RIGHT;
    auto properties = relDetail->kU_Properties() ?
                          transformProperties(*relDetail->kU_Properties()) :
                          vector<pair<string, unique_ptr<ParsedExpression>>>{};
    return make_unique<RelPattern>(
        variable, relTypes, lowerBound, upperBound, arrowHead, std::move(properties));
}

vector<pair<string, unique_ptr<ParsedExpression>>> Transformer::transformProperties(
    CypherParser::KU_PropertiesContext& ctx) {
    vector<pair<string, unique_ptr<ParsedExpression>>> result;
    assert(ctx.oC_PropertyKeyName().size() == ctx.oC_Expression().size());
    for (auto i = 0u; i < ctx.oC_PropertyKeyName().size(); ++i) {
        auto propertyKeyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName(i));
        auto expression = transformExpression(*ctx.oC_Expression(i));
        result.emplace_back(propertyKeyName, std::move(expression));
    }
    return result;
}

vector<string> Transformer::transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx) {
    vector<string> relTypes;
    for (auto& relType : ctx.oC_RelTypeName()) {
        relTypes.push_back(transformRelTypeName(*relType));
    }
    return relTypes;
}

vector<string> Transformer::transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx) {
    vector<string> nodeLabels;
    for (auto& nodeLabel : ctx.oC_NodeLabel()) {
        nodeLabels.push_back(transformNodeLabel(*nodeLabel));
    }
    return nodeLabels;
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
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " OR " + next->getRawName();
            expression =
                make_unique<ParsedExpression>(OR, std::move(expression), std::move(next), rawName);
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
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " XOR " + next->getRawName();
            expression =
                make_unique<ParsedExpression>(XOR, std::move(expression), std::move(next), rawName);
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
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " AND " + next->getRawName();
            expression =
                make_unique<ParsedExpression>(AND, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext& ctx) {
    if (ctx.NOT()) {
        return make_unique<ParsedExpression>(
            NOT, transformComparisonExpression(*ctx.oC_ComparisonExpression()), ctx.getText());
    }
    return transformComparisonExpression(*ctx.oC_ComparisonExpression());
}

unique_ptr<ParsedExpression> Transformer::transformComparisonExpression(
    CypherParser::OC_ComparisonExpressionContext& ctx) {
    if (1 == ctx.kU_BitwiseOrOperatorExpression().size()) {
        return transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    }
    // Antlr parser throws error for conjunctive comparison.
    // Transformer should only handle the case of single comparison operator.
    assert(ctx.kU_ComparisonOperator().size() == 1);
    auto left = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    auto right = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(1));
    auto comparisonOperator = ctx.kU_ComparisonOperator()[0]->getText();
    if (comparisonOperator == "=") {
        return make_unique<ParsedExpression>(
            EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<>") {
        return make_unique<ParsedExpression>(
            NOT_EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">") {
        return make_unique<ParsedExpression>(
            GREATER_THAN, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">=") {
        return make_unique<ParsedExpression>(
            GREATER_THAN_EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<") {
        return make_unique<ParsedExpression>(
            LESS_THAN, std::move(left), std::move(right), ctx.getText());
    } else {
        assert(comparisonOperator == "<=");
        return make_unique<ParsedExpression>(
            LESS_THAN_EQUALS, std::move(left), std::move(right), ctx.getText());
    }
}

unique_ptr<ParsedExpression> Transformer::transformBitwiseOrOperatorExpression(
    CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.kU_BitwiseAndOperatorExpression().size(); ++i) {
        auto next = transformBitwiseAndOperatorExpression(*ctx.kU_BitwiseAndOperatorExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " | " + next->getRawName();
            expression = make_unique<ParsedFunctionExpression>(
                BITWISE_OR_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformBitwiseAndOperatorExpression(
    CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.kU_BitShiftOperatorExpression().size(); ++i) {
        auto next = transformBitShiftOperatorExpression(*ctx.kU_BitShiftOperatorExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " & " + next->getRawName();
            expression = make_unique<ParsedFunctionExpression>(
                BITWISE_AND_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformBitShiftOperatorExpression(
    CypherParser::KU_BitShiftOperatorExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_AddOrSubtractExpression().size(); ++i) {
        auto next = transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto bitShiftOperator = ctx.kU_BitShiftOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + bitShiftOperator + " " + next->getRawName();
            if (bitShiftOperator == "<<") {
                expression = make_unique<ParsedFunctionExpression>(
                    BITSHIFT_LEFT_FUNC_NAME, std::move(expression), std::move(next), rawName);
            } else {
                assert(bitShiftOperator == ">>");
                expression = make_unique<ParsedFunctionExpression>(
                    BITSHIFT_RIGHT_FUNC_NAME, std::move(expression), std::move(next), rawName);
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next =
            transformMultiplyDivideModuloExpression(*ctx.oC_MultiplyDivideModuloExpression(i));
        if (!expression) {
            expression = std::move(next);
        } else {
            auto arithmeticOperator = ctx.kU_AddOrSubtractOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + arithmeticOperator + " " + next->getRawName();
            expression = make_unique<ParsedFunctionExpression>(
                arithmeticOperator, std::move(expression), std::move(next), rawName);
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
            expression = std::move(next);
        } else {
            auto arithmeticOperator = ctx.kU_MultiplyDivideModuloOperator(i - 1)->getText();
            auto rawName =
                expression->getRawName() + " " + arithmeticOperator + " " + next->getRawName();
            expression = make_unique<ParsedFunctionExpression>(
                arithmeticOperator, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext& ctx) {
    unique_ptr<ParsedExpression> expression;
    for (auto& unaryAddOrSubtractExpression : ctx.oC_UnaryAddSubtractOrFactorialExpression()) {
        auto next = transformUnaryAddSubtractOrFactorialExpression(*unaryAddOrSubtractExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            auto rawName = expression->getRawName() + " ^ " + next->getRawName();
            expression = make_unique<ParsedFunctionExpression>(
                POWER_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformUnaryAddSubtractOrFactorialExpression(
    CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx) {
    if (ctx.MINUS() && ctx.FACTORIAL()) {
        auto exp1 = make_unique<ParsedFunctionExpression>(FACTORIAL_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
        return make_unique<ParsedFunctionExpression>(
            NEGATE_FUNC_NAME, std::move(exp1), ctx.getText());
    } else if (ctx.MINUS()) {
        return make_unique<ParsedFunctionExpression>(NEGATE_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
    } else if (ctx.FACTORIAL()) {
        return make_unique<ParsedFunctionExpression>(FACTORIAL_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
    }
    return transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression());
}

unique_ptr<ParsedExpression> Transformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext& ctx) {
    auto propertyExpression =
        transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(
            *ctx.oC_NullOperatorExpression(), std::move(propertyExpression));
    }
    if (ctx.oC_ListOperatorExpression()) {
        return transformListOperatorExpression(
            *ctx.oC_ListOperatorExpression(), std::move(propertyExpression));
    }
    if (ctx.oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(
            *ctx.oC_StringOperatorExpression(), std::move(propertyExpression));
    }
    return propertyExpression;
}

unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.STARTS()) {
        return make_unique<ParsedFunctionExpression>(
            STARTS_WITH_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    } else if (ctx.ENDS()) {
        return make_unique<ParsedFunctionExpression>(
            ENDS_WITH_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    } else {
        assert(ctx.CONTAINS());
        return make_unique<ParsedFunctionExpression>(
            CONTAINS_FUNC_NAME, std::move(propertyExpression), std::move(right), rawExpression);
    }
}

unique_ptr<ParsedLiteralExpression> getZeroLiteral() {
    auto literal = make_unique<Literal>((int64_t)0);
    return make_unique<ParsedLiteralExpression>(std::move(literal), "0");
}

unique_ptr<ParsedExpression> Transformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    unique_ptr<ParsedExpression> listOperator;
    if (ctx.kU_ListSliceOperatorExpression()) {
        listOperator = transformListSliceOperatorExpression(
            *ctx.kU_ListSliceOperatorExpression(), std::move(propertyExpression));
    } else {
        listOperator = transformListExtractOperatorExpression(
            *ctx.kU_ListExtractOperatorExpression(), std::move(propertyExpression));
    }
    if (ctx.oC_ListOperatorExpression()) {
        return transformListOperatorExpression(
            *ctx.oC_ListOperatorExpression(), std::move(listOperator));
    } else {
        return listOperator;
    }
}

unique_ptr<ParsedExpression> Transformer::transformListSliceOperatorExpression(
    CypherParser::KU_ListSliceOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto listSlice =
        make_unique<ParsedFunctionExpression>(LIST_SLICE_FUNC_NAME, std::move(rawExpression));
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
};

unique_ptr<ParsedExpression> Transformer::transformListExtractOperatorExpression(
    CypherParser::KU_ListExtractOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto listExtract =
        make_unique<ParsedFunctionExpression>(LIST_EXTRACT_FUNC_NAME, std::move(rawExpression));
    listExtract->addChild(std::move(propertyExpression));
    listExtract->addChild(transformExpression(*ctx.oC_Expression()));
    return listExtract;
};

unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext& ctx,
    unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    assert(ctx.IS() && ctx.NULL_());
    return ctx.NOT() ?
               make_unique<ParsedExpression>(
                   IS_NOT_NULL, std::move(propertyExpression), rawExpression) :
               make_unique<ParsedExpression>(IS_NULL, std::move(propertyExpression), rawExpression);
}

unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext& ctx) {
    if (ctx.oC_PropertyLookup()) {
        return make_unique<ParsedPropertyExpression>(PROPERTY,
            transformPropertyLookup(*ctx.oC_PropertyLookup()), transformAtom(*ctx.oC_Atom()),
            ctx.getText());
    }
    return transformAtom(*ctx.oC_Atom());
}

unique_ptr<ParsedExpression> Transformer::transformAtom(CypherParser::OC_AtomContext& ctx) {
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
    } else if (ctx.oC_ExistentialSubquery()) {
        return transformExistentialSubquery(*ctx.oC_ExistentialSubquery());
    } else {
        assert(ctx.oC_Variable());
        return make_unique<ParsedVariableExpression>(
            transformVariable(*ctx.oC_Variable()), ctx.getText());
    }
}

unique_ptr<ParsedExpression> Transformer::transformLiteral(CypherParser::OC_LiteralContext& ctx) {
    if (ctx.oC_NumberLiteral()) {
        return transformNumberLiteral(*ctx.oC_NumberLiteral());
    } else if (ctx.oC_BooleanLiteral()) {
        return transformBooleanLiteral(*ctx.oC_BooleanLiteral());
    } else if (ctx.StringLiteral()) {
        return make_unique<ParsedLiteralExpression>(
            make_unique<Literal>(transformStringLiteral(*ctx.StringLiteral())), ctx.getText());
    } else if (ctx.NULL_()) {
        return make_unique<ParsedLiteralExpression>(make_unique<Literal>(), ctx.getText());
    } else {
        assert(ctx.oC_ListLiteral());
        return transformListLiteral(*ctx.oC_ListLiteral());
    }
}

unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext& ctx) {
    unique_ptr<Literal> literal;
    if (ctx.TRUE()) {
        literal = make_unique<Literal>(true);
    } else if (ctx.FALSE()) {
        literal = make_unique<Literal>(false);
    }
    assert(literal);
    return make_unique<ParsedLiteralExpression>(std::move(literal), ctx.getText());
}

unique_ptr<ParsedExpression> Transformer::transformListLiteral(
    CypherParser::OC_ListLiteralContext& ctx) {
    auto listCreation =
        make_unique<ParsedFunctionExpression>(LIST_CREATION_FUNC_NAME, ctx.getText());
    for (auto& childExpr : ctx.oC_Expression()) {
        listCreation->addChild(transformExpression(*childExpr));
    }
    return listCreation;
}

unique_ptr<ParsedExpression> Transformer::transformParameterExpression(
    CypherParser::OC_ParameterContext& ctx) {
    auto parameterName =
        ctx.oC_SymbolicName() ? ctx.oC_SymbolicName()->getText() : ctx.DecimalInteger()->getText();
    return make_unique<ParsedParameterExpression>(parameterName, ctx.getText());
}

unique_ptr<ParsedExpression> Transformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

unique_ptr<ParsedExpression> Transformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext& ctx) {
    auto functionName = transformFunctionName(*ctx.oC_FunctionName());
    if (ctx.STAR()) {
        StringUtils::toUpper(functionName);
        assert(functionName == "COUNT");
        return make_unique<ParsedFunctionExpression>(COUNT_STAR_FUNC_NAME, ctx.getText());
    }
    auto expression = make_unique<ParsedFunctionExpression>(
        functionName, ctx.getText(), ctx.DISTINCT() != nullptr);
    for (auto& childExpr : ctx.oC_Expression()) {
        expression->addChild(transformExpression(*childExpr));
    }
    return expression;
}

string Transformer::transformFunctionName(CypherParser::OC_FunctionNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

unique_ptr<ParsedExpression> Transformer::transformExistentialSubquery(
    CypherParser::OC_ExistentialSubqueryContext& ctx) {
    auto existsSubquery =
        make_unique<ParsedSubqueryExpression>(transformPattern(*ctx.oC_Pattern()), ctx.getText());
    if (ctx.oC_Where()) {
        existsSubquery->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return existsSubquery;
}

string Transformer::transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

unique_ptr<ParsedExpression> Transformer::transformCaseExpression(
    CypherParser::OC_CaseExpressionContext& ctx) {
    unique_ptr<ParsedExpression> caseExpression = nullptr;
    unique_ptr<ParsedExpression> elseExpression = nullptr;
    if (ctx.ELSE()) {
        if (ctx.oC_Expression().size() == 1) {
            elseExpression = transformExpression(*ctx.oC_Expression(0));
        } else {
            assert(ctx.oC_Expression().size() == 2);
            caseExpression = transformExpression(*ctx.oC_Expression(0));
            elseExpression = transformExpression(*ctx.oC_Expression(1));
        }
    } else {
        if (ctx.oC_Expression().size() == 1) {
            caseExpression = transformExpression(*ctx.oC_Expression(0));
        }
    }
    auto parsedCaseExpression = make_unique<ParsedCaseExpression>(ctx.getText());
    parsedCaseExpression->setCaseExpression(std::move(caseExpression));
    parsedCaseExpression->setElseExpression(std::move(elseExpression));
    for (auto& caseAlternative : ctx.oC_CaseAlternative()) {
        parsedCaseExpression->addCaseAlternative(transformCaseAlternative(*caseAlternative));
    }
    return parsedCaseExpression;
}

unique_ptr<ParsedCaseAlternative> Transformer::transformCaseAlternative(
    CypherParser::OC_CaseAlternativeContext& ctx) {
    auto whenExpression = transformExpression(*ctx.oC_Expression(0));
    auto thenExpression = transformExpression(*ctx.oC_Expression(1));
    return make_unique<ParsedCaseAlternative>(std::move(whenExpression), std::move(thenExpression));
}

string Transformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext& ctx) {
    if (ctx.oC_IntegerLiteral()) {
        return transformIntegerLiteral(*ctx.oC_IntegerLiteral());
    } else {
        assert(ctx.oC_DoubleLiteral());
        return transformDoubleLiteral(*ctx.oC_DoubleLiteral());
    }
}

unique_ptr<ParsedExpression> Transformer::transformProperty(
    CypherParser::OC_PropertyExpressionContext& ctx) {
    return make_unique<ParsedPropertyExpression>(PROPERTY,
        transformPropertyLookup(*ctx.oC_PropertyLookup()), transformAtom(*ctx.oC_Atom()),
        ctx.getText());
}

string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(
    CypherParser::OC_IntegerLiteralContext& ctx) {
    auto literal =
        make_unique<Literal>(TypeUtils::convertToInt64(ctx.DecimalInteger()->getText().c_str()));
    return make_unique<ParsedLiteralExpression>(std::move(literal), ctx.getText());
}

unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(
    CypherParser::OC_DoubleLiteralContext& ctx) {
    auto literal = make_unique<Literal>(
        TypeUtils::convertToDouble(ctx.RegularDecimalReal()->getText().c_str()));
    return make_unique<ParsedLiteralExpression>(std::move(literal), ctx.getText());
}

string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    if (ctx.UnescapedSymbolicName()) {
        return ctx.UnescapedSymbolicName()->getText();
    } else if (ctx.EscapedSymbolicName()) {
        return ctx.EscapedSymbolicName()->getText();
    } else {
        assert(ctx.HexLetter());
        return ctx.HexLetter()->getText();
    }
}

unique_ptr<Statement> Transformer::transformDDL() {
    if (root.kU_DDL()->kU_CreateNode()) {
        return transformCreateNodeClause(*root.kU_DDL()->kU_CreateNode());
    } else if (root.kU_DDL()->kU_CreateRel()) {
        return transformCreateRelClause(*root.kU_DDL()->kU_CreateRel());
    } else {
        return transformDropTable(*root.kU_DDL()->kU_DropTable());
    }
}

unique_ptr<Statement> Transformer::transformCreateNodeClause(
    CypherParser::KU_CreateNodeContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    auto pkColName =
        ctx.kU_CreateNodeConstraint() ? transformPrimaryKey(*ctx.kU_CreateNodeConstraint()) : "";
    return make_unique<CreateNodeClause>(
        std::move(schemaName), std::move(propertyDefinitions), pkColName);
}

unique_ptr<Statement> Transformer::transformCreateRelClause(
    CypherParser::KU_CreateRelContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyDefinitions = ctx.kU_PropertyDefinitions() ?
                                   transformPropertyDefinitions(*ctx.kU_PropertyDefinitions()) :
                                   vector<pair<string, string>>();
    auto relMultiplicity =
        ctx.oC_SymbolicName() ? transformSymbolicName(*ctx.oC_SymbolicName()) : "MANY_MANY";
    auto relConnections = transformRelConnections(*ctx.kU_RelConnections());
    return make_unique<CreateRelClause>(std::move(schemaName), std::move(propertyDefinitions),
        relMultiplicity, std::move(relConnections));
}

unique_ptr<Statement> Transformer::transformDropTable(CypherParser::KU_DropTableContext& ctx) {
    return make_unique<DropTable>(transformSchemaName(*ctx.oC_SchemaName()));
}

vector<pair<string, string>> Transformer::transformPropertyDefinitions(
    CypherParser::KU_PropertyDefinitionsContext& ctx) {
    vector<pair<string, string>> propertyNameDataTypes;
    for (auto property : ctx.kU_PropertyDefinition()) {
        propertyNameDataTypes.emplace_back(
            transformPropertyKeyName(*property->oC_PropertyKeyName()),
            transformDataType(*property->kU_DataType()));
    }
    return propertyNameDataTypes;
}

string Transformer::transformDataType(CypherParser::KU_DataTypeContext& ctx) {
    auto dataType = transformSymbolicName(*ctx.oC_SymbolicName());
    if (ctx.kU_ListIdentifiers()) {
        dataType += transformListIdentifiers(*ctx.kU_ListIdentifiers());
    }
    return dataType;
}

string Transformer::transformListIdentifiers(CypherParser::KU_ListIdentifiersContext& ctx) {
    string listIdentifiers;
    for (auto& listIdentifier : ctx.kU_ListIdentifier()) {
        listIdentifiers += listIdentifier->getText();
    }
    return listIdentifiers;
}

string Transformer::transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

vector<pair<string, string>> Transformer::transformRelConnections(
    CypherParser::KU_RelConnectionsContext& ctx) {
    vector<pair<string, string>> relConnections;
    if (!ctx.kU_RelConnection().empty()) {
        for (auto& relConnection : ctx.kU_RelConnection()) {
            auto newSrcTableNames = transformNodeLabels(*relConnection->kU_NodeLabels()[0]);
            auto newDstTableNames = transformNodeLabels(*relConnection->kU_NodeLabels()[1]);
            for (auto& newSrcTableName : newSrcTableNames) {
                for (auto& newDstTableName : newDstTableNames) {
                    relConnections.emplace_back(newSrcTableName, newDstTableName);
                }
            }
        }
    }
    return relConnections;
}

vector<string> Transformer::transformNodeLabels(CypherParser::KU_NodeLabelsContext& ctx) {
    vector<string> nodeLabels;
    for (auto& nodeLabel : ctx.oC_SchemaName()) {
        nodeLabels.push_back(transformSchemaName(*nodeLabel));
    }
    return nodeLabels;
}

unique_ptr<Statement> Transformer::transformCopyCSV() {
    auto& ctx = *root.kU_CopyCSV();
    auto csvFileName = transformStringLiteral(*ctx.StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto parsingOptions = ctx.kU_ParsingOptions() ?
                              transformParsingOptions(*ctx.kU_ParsingOptions()) :
                              unordered_map<string, unique_ptr<ParsedExpression>>();
    return make_unique<CopyCSV>(
        std::move(csvFileName), std::move(tableName), std::move(parsingOptions));
}

unordered_map<string, unique_ptr<ParsedExpression>> Transformer::transformParsingOptions(
    CypherParser::KU_ParsingOptionsContext& ctx) {
    unordered_map<string, unique_ptr<ParsedExpression>> copyOptions;
    for (auto loadOption : ctx.kU_ParsingOption()) {
        auto optionName = transformSymbolicName(*loadOption->oC_SymbolicName());
        copyOptions.emplace(optionName, transformLiteral(*loadOption->oC_Literal()));
    }
    return copyOptions;
}

string Transformer::transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral) {
    auto str = stringLiteral.getText();
    return str.substr(1, str.size() - 2);
}

} // namespace parser
} // namespace kuzu
