#include "parser/transformer.h"

#include "common/copier_config/copier_config.h"
#include "common/string_utils.h"
#include "parser/call/call.h"
#include "parser/copy.h"
#include "parser/ddl/add_property.h"
#include "parser/ddl/create_node_clause.h"
#include "parser/ddl/create_rel_clause.h"
#include "parser/ddl/drop_property.h"
#include "parser/ddl/drop_table.h"
#include "parser/ddl/rename_property.h"
#include "parser/ddl/rename_table.h"
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

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transform() {
    auto statement = transformOcStatement(*root.oC_Statement());
    if (root.oC_AnyCypherOption()) {
        auto cypherOption = root.oC_AnyCypherOption();
        if (cypherOption->oC_Explain()) {
            statement->enableExplain();
        }
        if (cypherOption->oC_Profile()) {
            statement->enableProfile();
        }
    }
    return statement;
}

std::unique_ptr<Statement> Transformer::transformOcStatement(
    CypherParser::OC_StatementContext& ctx) {
    if (ctx.oC_Query()) {
        return transformQuery(*ctx.oC_Query());
    } else if (ctx.kU_DDL()) {
        return transformDDL(*ctx.kU_DDL());
    } else if (ctx.kU_CopyNPY()) {
        return transformCopyNPY(*ctx.kU_CopyNPY());
    } else if (ctx.kU_CopyCSV()) {
        return transformCopyCSV(*ctx.kU_CopyCSV());
    } else {
        return transformCall(*ctx.kU_Call());
    }
}

std::unique_ptr<RegularQuery> Transformer::transformQuery(CypherParser::OC_QueryContext& ctx) {
    return transformRegularQuery(*ctx.oC_RegularQuery());
}

std::unique_ptr<RegularQuery> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext& ctx) {
    auto regularQuery = std::make_unique<RegularQuery>(transformSingleQuery(*ctx.oC_SingleQuery()));
    for (auto unionClause : ctx.oC_Union()) {
        regularQuery->addSingleQuery(
            transformSingleQuery(*unionClause->oC_SingleQuery()), unionClause->ALL());
    }
    return regularQuery;
}

std::unique_ptr<SingleQuery> Transformer::transformSingleQuery(
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

std::unique_ptr<SingleQuery> Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext& ctx) {
    auto singleQuery = std::make_unique<SingleQuery>();
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

std::unique_ptr<QueryPart> Transformer::transformQueryPart(CypherParser::KU_QueryPartContext& ctx) {
    auto queryPart = std::make_unique<QueryPart>(transformWith(*ctx.oC_With()));
    for (auto& readingClause : ctx.oC_ReadingClause()) {
        queryPart->addReadingClause(transformReadingClause(*readingClause));
    }
    for (auto& updatingClause : ctx.oC_UpdatingClause()) {
        queryPart->addUpdatingClause(transformUpdatingClause(*updatingClause));
    }
    return queryPart;
}

std::unique_ptr<UpdatingClause> Transformer::transformUpdatingClause(
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

std::unique_ptr<ReadingClause> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    if (ctx.oC_Match()) {
        return transformMatch(*ctx.oC_Match());
    } else {
        assert(ctx.oC_Unwind());
        return transformUnwind(*ctx.oC_Unwind());
    }
}

std::unique_ptr<ReadingClause> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchClause =
        std::make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()), ctx.OPTIONAL());
    if (ctx.oC_Where()) {
        matchClause->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return matchClause;
}

std::unique_ptr<ReadingClause> Transformer::transformUnwind(CypherParser::OC_UnwindContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    auto transformedVariable = transformVariable(*ctx.oC_Variable());
    return std::make_unique<UnwindClause>(std::move(expression), std::move(transformedVariable));
}

std::unique_ptr<UpdatingClause> Transformer::transformCreate(CypherParser::OC_CreateContext& ctx) {
    return std::make_unique<CreateClause>(transformPattern(*ctx.oC_Pattern()));
}

std::unique_ptr<UpdatingClause> Transformer::transformSet(CypherParser::OC_SetContext& ctx) {
    auto setClause = std::make_unique<SetClause>();
    for (auto& setItem : ctx.oC_SetItem()) {
        setClause->addSetItem(transformSetItem(*setItem));
    }
    return setClause;
}

std::pair<std::unique_ptr<ParsedExpression>, std::unique_ptr<ParsedExpression>>
Transformer::transformSetItem(CypherParser::OC_SetItemContext& ctx) {
    return make_pair(
        transformProperty(*ctx.oC_PropertyExpression()), transformExpression(*ctx.oC_Expression()));
}

std::unique_ptr<UpdatingClause> Transformer::transformDelete(CypherParser::OC_DeleteContext& ctx) {
    auto deleteClause = std::make_unique<DeleteClause>();
    for (auto& expression : ctx.oC_Expression()) {
        deleteClause->addExpression(transformExpression(*expression));
    }
    return deleteClause;
}

std::unique_ptr<WithClause> Transformer::transformWith(CypherParser::OC_WithContext& ctx) {
    auto withClause =
        std::make_unique<WithClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
    if (ctx.oC_Where()) {
        withClause->setWhereExpression(transformWhere(*ctx.oC_Where()));
    }
    return withClause;
}

std::unique_ptr<ReturnClause> Transformer::transformReturn(CypherParser::OC_ReturnContext& ctx) {
    return std::make_unique<ReturnClause>(transformProjectionBody(*ctx.oC_ProjectionBody()));
}

std::unique_ptr<ProjectionBody> Transformer::transformProjectionBody(
    CypherParser::OC_ProjectionBodyContext& ctx) {
    auto projectionBody = std::make_unique<ProjectionBody>(nullptr != ctx.DISTINCT(),
        nullptr != ctx.oC_ProjectionItems()->STAR(),
        transformProjectionItems(*ctx.oC_ProjectionItems()));
    if (ctx.oC_Order()) {
        std::vector<std::unique_ptr<ParsedExpression>> orderByExpressions;
        std::vector<bool> isAscOrders;
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

std::vector<std::unique_ptr<ParsedExpression>> Transformer::transformProjectionItems(
    CypherParser::OC_ProjectionItemsContext& ctx) {
    std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions;
    for (auto& projectionItem : ctx.oC_ProjectionItem()) {
        projectionExpressions.push_back(transformProjectionItem(*projectionItem));
    }
    return projectionExpressions;
}

std::unique_ptr<ParsedExpression> Transformer::transformProjectionItem(
    CypherParser::OC_ProjectionItemContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    if (ctx.AS()) {
        expression->setAlias(transformVariable(*ctx.oC_Variable()));
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformWhere(CypherParser::OC_WhereContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

std::vector<std::unique_ptr<PatternElement>> Transformer::transformPattern(
    CypherParser::OC_PatternContext& ctx) {
    std::vector<std::unique_ptr<PatternElement>> pattern;
    for (auto& patternPart : ctx.oC_PatternPart()) {
        pattern.push_back(transformPatternPart(*patternPart));
    }
    return pattern;
}

std::unique_ptr<PatternElement> Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext& ctx) {
    return transformAnonymousPatternPart(*ctx.oC_AnonymousPatternPart());
}

std::unique_ptr<PatternElement> Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext& ctx) {
    return transformPatternElement(*ctx.oC_PatternElement());
}

std::unique_ptr<PatternElement> Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext& ctx) {
    if (ctx.oC_PatternElement()) { // parenthesized pattern element
        return transformPatternElement(*ctx.oC_PatternElement());
    }
    auto patternElement =
        std::make_unique<PatternElement>(transformNodePattern(*ctx.oC_NodePattern()));
    if (!ctx.oC_PatternElementChain().empty()) {
        for (auto& patternElementChain : ctx.oC_PatternElementChain()) {
            patternElement->addPatternElementChain(
                transformPatternElementChain(*patternElementChain));
        }
    }
    return patternElement;
}

std::unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
    auto variable = ctx.oC_Variable() ? transformVariable(*ctx.oC_Variable()) : std::string();
    auto nodeLabels = ctx.oC_NodeLabels() ? transformNodeLabels(*ctx.oC_NodeLabels()) :
                                            std::vector<std::string>{};
    auto properties = ctx.kU_Properties() ?
                          transformProperties(*ctx.kU_Properties()) :
                          std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
    return std::make_unique<NodePattern>(
        std::move(variable), std::move(nodeLabels), std::move(properties));
}

std::unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext& ctx) {
    return std::make_unique<PatternElementChain>(
        transformRelationshipPattern(*ctx.oC_RelationshipPattern()),
        transformNodePattern(*ctx.oC_NodePattern()));
}

std::unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
    auto relDetail = ctx.oC_RelationshipDetail();
    auto variable =
        relDetail->oC_Variable() ? transformVariable(*relDetail->oC_Variable()) : std::string();
    auto relTypes = relDetail->oC_RelationshipTypes() ?
                        transformRelTypes(*relDetail->oC_RelationshipTypes()) :
                        std::vector<std::string>{};
    auto relType = common::QueryRelType::NON_RECURSIVE;
    std::string lowerBound = "1";
    std::string upperBound = "1";
    if (relDetail->oC_RangeLiteral()) {
        lowerBound = relDetail->oC_RangeLiteral()->oC_IntegerLiteral()[0]->getText();
        upperBound = relDetail->oC_RangeLiteral()->oC_IntegerLiteral()[1]->getText();
        if (relDetail->oC_RangeLiteral()->ALL()) {
            relType = common::QueryRelType::ALL_SHORTEST;
        } else if (relDetail->oC_RangeLiteral()->SHORTEST()) {
            relType = common::QueryRelType::SHORTEST;
        } else {
            relType = common::QueryRelType::VARIABLE_LENGTH;
        }
    }
    ArrowDirection arrowDirection;
    if (ctx.oC_LeftArrowHead()) {
        arrowDirection = ArrowDirection::LEFT;
    } else if (ctx.oC_RightArrowHead()) {
        arrowDirection = ArrowDirection::RIGHT;
    } else {
        arrowDirection = ArrowDirection::BOTH;
    }
    auto properties = relDetail->kU_Properties() ?
                          transformProperties(*relDetail->kU_Properties()) :
                          std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
    return std::make_unique<RelPattern>(
        variable, relTypes, relType, lowerBound, upperBound, arrowDirection, std::move(properties));
}

std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>
Transformer::transformProperties(CypherParser::KU_PropertiesContext& ctx) {
    std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> result;
    assert(ctx.oC_PropertyKeyName().size() == ctx.oC_Expression().size());
    for (auto i = 0u; i < ctx.oC_PropertyKeyName().size(); ++i) {
        auto propertyKeyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName(i));
        auto expression = transformExpression(*ctx.oC_Expression(i));
        result.emplace_back(propertyKeyName, std::move(expression));
    }
    return result;
}

std::vector<std::string> Transformer::transformRelTypes(
    CypherParser::OC_RelationshipTypesContext& ctx) {
    std::vector<std::string> relTypes;
    for (auto& relType : ctx.oC_RelTypeName()) {
        relTypes.push_back(transformRelTypeName(*relType));
    }
    return relTypes;
}

std::vector<std::string> Transformer::transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx) {
    std::vector<std::string> nodeLabels;
    for (auto& nodeLabel : ctx.oC_NodeLabel()) {
        nodeLabels.push_back(transformNodeLabel(*nodeLabel));
    }
    return nodeLabels;
}

std::string Transformer::transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx) {
    return transformLabelName(*ctx.oC_LabelName());
}

std::string Transformer::transformLabelName(CypherParser::OC_LabelNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

std::string Transformer::transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

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
                common::ExpressionType::OR, std::move(expression), std::move(next), rawName);
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
                common::ExpressionType::XOR, std::move(expression), std::move(next), rawName);
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
                common::ExpressionType::AND, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformNotExpression(
    CypherParser::OC_NotExpressionContext& ctx) {
    if (ctx.NOT()) {
        return std::make_unique<ParsedExpression>(common::ExpressionType::NOT,
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
    assert(ctx.kU_ComparisonOperator().size() == 1);
    auto left = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    auto right = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(1));
    auto comparisonOperator = ctx.kU_ComparisonOperator()[0]->getText();
    if (comparisonOperator == "=") {
        return std::make_unique<ParsedExpression>(
            common::ExpressionType::EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<>") {
        return std::make_unique<ParsedExpression>(
            common::ExpressionType::NOT_EQUALS, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">") {
        return std::make_unique<ParsedExpression>(
            common::ExpressionType::GREATER_THAN, std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == ">=") {
        return std::make_unique<ParsedExpression>(common::ExpressionType::GREATER_THAN_EQUALS,
            std::move(left), std::move(right), ctx.getText());
    } else if (comparisonOperator == "<") {
        return std::make_unique<ParsedExpression>(
            common::ExpressionType::LESS_THAN, std::move(left), std::move(right), ctx.getText());
    } else {
        assert(comparisonOperator == "<=");
        return std::make_unique<ParsedExpression>(common::ExpressionType::LESS_THAN_EQUALS,
            std::move(left), std::move(right), ctx.getText());
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
                common::BITWISE_OR_FUNC_NAME, std::move(expression), std::move(next), rawName);
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
                common::BITWISE_AND_FUNC_NAME, std::move(expression), std::move(next), rawName);
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
                expression =
                    std::make_unique<ParsedFunctionExpression>(common::BITSHIFT_LEFT_FUNC_NAME,
                        std::move(expression), std::move(next), rawName);
            } else {
                assert(bitShiftOperator == ">>");
                expression =
                    std::make_unique<ParsedFunctionExpression>(common::BITSHIFT_RIGHT_FUNC_NAME,
                        std::move(expression), std::move(next), rawName);
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
                common::POWER_FUNC_NAME, std::move(expression), std::move(next), rawName);
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformUnaryAddSubtractOrFactorialExpression(
    CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx) {
    if (ctx.MINUS() && ctx.FACTORIAL()) {
        auto exp1 = std::make_unique<ParsedFunctionExpression>(common::FACTORIAL_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
        return std::make_unique<ParsedFunctionExpression>(
            common::NEGATE_FUNC_NAME, std::move(exp1), ctx.getText());
    } else if (ctx.MINUS()) {
        return std::make_unique<ParsedFunctionExpression>(common::NEGATE_FUNC_NAME,
            transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression()),
            ctx.getText());
    } else if (ctx.FACTORIAL()) {
        return std::make_unique<ParsedFunctionExpression>(common::FACTORIAL_FUNC_NAME,
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

std::unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    if (ctx.STARTS()) {
        return std::make_unique<ParsedFunctionExpression>(common::STARTS_WITH_FUNC_NAME,
            std::move(propertyExpression), std::move(right), rawExpression);
    } else if (ctx.ENDS()) {
        return std::make_unique<ParsedFunctionExpression>(common::ENDS_WITH_FUNC_NAME,
            std::move(propertyExpression), std::move(right), rawExpression);
    } else if (ctx.CONTAINS()) {
        return std::make_unique<ParsedFunctionExpression>(common::CONTAINS_FUNC_NAME,
            std::move(propertyExpression), std::move(right), rawExpression);
    } else {
        assert(ctx.oC_RegularExpression());
        return std::make_unique<ParsedFunctionExpression>(common::REGEXP_FULL_MATCH_FUNC_NAME,
            std::move(propertyExpression), std::move(right), rawExpression);
    }
}

static std::unique_ptr<ParsedLiteralExpression> getZeroLiteral() {
    auto literal = std::make_unique<common::Value>(0);
    return std::make_unique<ParsedLiteralExpression>(std::move(literal), "0");
}

std::unique_ptr<ParsedExpression> Transformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    std::unique_ptr<ParsedExpression> listOperator;
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

std::unique_ptr<ParsedExpression> Transformer::transformListSliceOperatorExpression(
    CypherParser::KU_ListSliceOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    auto listSlice = std::make_unique<ParsedFunctionExpression>(
        common::LIST_SLICE_FUNC_NAME, std::move(rawExpression));
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
        common::LIST_EXTRACT_FUNC_NAME, std::move(rawExpression));
    listExtract->addChild(std::move(propertyExpression));
    listExtract->addChild(transformExpression(*ctx.oC_Expression()));
    return listExtract;
}

std::unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext& ctx,
    std::unique_ptr<ParsedExpression> propertyExpression) {
    auto rawExpression = propertyExpression->getRawName() + " " + ctx.getText();
    assert(ctx.IS() && ctx.NULL_());
    return ctx.NOT() ? std::make_unique<ParsedExpression>(common::ExpressionType::IS_NOT_NULL,
                           std::move(propertyExpression), rawExpression) :
                       std::make_unique<ParsedExpression>(common::ExpressionType::IS_NULL,
                           std::move(propertyExpression), rawExpression);
}

std::unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext& ctx) {
    if (ctx.oC_PropertyLookup()) {
        return std::make_unique<ParsedPropertyExpression>(common::ExpressionType::PROPERTY,
            transformPropertyLookup(*ctx.oC_PropertyLookup()), transformAtom(*ctx.oC_Atom()),
            ctx.getText());
    }
    return transformAtom(*ctx.oC_Atom());
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
    } else if (ctx.oC_ExistentialSubquery()) {
        return transformExistentialSubquery(*ctx.oC_ExistentialSubquery());
    } else {
        assert(ctx.oC_Variable());
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
            std::make_unique<common::Value>(common::LogicalType{common::LogicalTypeID::STRING},
                transformStringLiteral(*ctx.StringLiteral())),
            ctx.getText());
    } else if (ctx.NULL_()) {
        return std::make_unique<ParsedLiteralExpression>(
            std::make_unique<common::Value>(common::Value::createNullValue()), ctx.getText());
    } else if (ctx.kU_StructLiteral()) {
        return transformStructLiteral(*ctx.kU_StructLiteral());
    } else {
        assert(ctx.oC_ListLiteral());
        return transformListLiteral(*ctx.oC_ListLiteral());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(
    CypherParser::OC_BooleanLiteralContext& ctx) {
    std::unique_ptr<common::Value> literal;
    if (ctx.TRUE()) {
        literal = std::make_unique<common::Value>(true);
    } else if (ctx.FALSE()) {
        literal = std::make_unique<common::Value>(false);
    }
    assert(literal);
    return std::make_unique<ParsedLiteralExpression>(std::move(literal), ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformListLiteral(
    CypherParser::OC_ListLiteralContext& ctx) {
    auto listCreation =
        std::make_unique<ParsedFunctionExpression>(common::LIST_CREATION_FUNC_NAME, ctx.getText());
    for (auto& childExpr : ctx.oC_Expression()) {
        listCreation->addChild(transformExpression(*childExpr));
    }
    return listCreation;
}

std::unique_ptr<ParsedExpression> Transformer::transformStructLiteral(
    CypherParser::KU_StructLiteralContext& ctx) {
    auto structPack =
        std::make_unique<ParsedFunctionExpression>(common::STRUCT_PACK_FUNC_NAME, ctx.getText());
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
    auto functionName = transformFunctionName(*ctx.oC_FunctionName());
    if (ctx.STAR()) {
        common::StringUtils::toUpper(functionName);
        assert(functionName == "COUNT");
        return std::make_unique<ParsedFunctionExpression>(
            common::COUNT_STAR_FUNC_NAME, ctx.getText());
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

std::unique_ptr<ParsedExpression> Transformer::transformExistentialSubquery(
    CypherParser::OC_ExistentialSubqueryContext& ctx) {
    auto existsSubquery = std::make_unique<ParsedSubqueryExpression>(
        transformPattern(*ctx.oC_Pattern()), ctx.getText());
    if (ctx.oC_Where()) {
        existsSubquery->setWhereClause(transformWhere(*ctx.oC_Where()));
    }
    return existsSubquery;
}

std::string Transformer::transformPropertyLookup(CypherParser::OC_PropertyLookupContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

std::unique_ptr<ParsedExpression> Transformer::transformCaseExpression(
    CypherParser::OC_CaseExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> caseExpression = nullptr;
    std::unique_ptr<ParsedExpression> elseExpression = nullptr;
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

std::string Transformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(
    CypherParser::OC_NumberLiteralContext& ctx) {
    if (ctx.oC_IntegerLiteral()) {
        return transformIntegerLiteral(*ctx.oC_IntegerLiteral());
    } else {
        assert(ctx.oC_DoubleLiteral());
        return transformDoubleLiteral(*ctx.oC_DoubleLiteral());
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformProperty(
    CypherParser::OC_PropertyExpressionContext& ctx) {
    return std::make_unique<ParsedPropertyExpression>(common::ExpressionType::PROPERTY,
        transformPropertyLookup(*ctx.oC_PropertyLookup()), transformAtom(*ctx.oC_Atom()),
        ctx.getText());
}

std::string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

std::unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(
    CypherParser::OC_IntegerLiteralContext& ctx) {
    auto value = std::make_unique<common::Value>(
        common::TypeUtils::convertStringToNumber<int64_t>(ctx.DecimalInteger()->getText().c_str()));
    return std::make_unique<ParsedLiteralExpression>(std::move(value), ctx.getText());
}

std::unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(
    CypherParser::OC_DoubleLiteralContext& ctx) {
    auto value = std::make_unique<common::Value>(common::TypeUtils::convertStringToNumber<double_t>(
        ctx.RegularDecimalReal()->getText().c_str()));
    return std::make_unique<ParsedLiteralExpression>(std::move(value), ctx.getText());
}

std::string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    if (ctx.UnescapedSymbolicName()) {
        return ctx.UnescapedSymbolicName()->getText();
    } else if (ctx.EscapedSymbolicName()) {
        std::string escapedSymbolName = ctx.EscapedSymbolicName()->getText();
        // escapedSymbolName symbol will be of form "`Some.Value`". Therefore, we need to sanitize
        // it such that we don't store the symbol with escape character.
        return escapedSymbolName.substr(1, escapedSymbolName.size() - 2);
    } else {
        assert(ctx.HexLetter());
        return ctx.HexLetter()->getText();
    }
}

std::unique_ptr<Statement> Transformer::transformDDL(CypherParser::KU_DDLContext& ctx) {
    if (ctx.kU_CreateNode()) {
        return transformCreateNodeClause(*ctx.kU_CreateNode());
    } else if (root.oC_Statement()->kU_DDL()->kU_CreateRel()) {
        return transformCreateRelClause(*root.oC_Statement()->kU_DDL()->kU_CreateRel());
    } else if (root.oC_Statement()->kU_DDL()->kU_DropTable()) {
        return transformDropTable(*root.oC_Statement()->kU_DDL()->kU_DropTable());
    } else {
        return transformAlterTable(*root.oC_Statement()->kU_DDL()->kU_AlterTable());
    }
}

std::unique_ptr<Statement> Transformer::transformAlterTable(
    CypherParser::KU_AlterTableContext& ctx) {
    if (ctx.kU_AlterOptions()->kU_AddProperty()) {
        return transformAddProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_DropProperty()) {
        return transformDropProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_RenameTable()) {
        return transformRenameTable(ctx);
    } else {
        return transformRenameProperty(ctx);
    }
}

std::unique_ptr<Statement> Transformer::transformCreateNodeClause(
    CypherParser::KU_CreateNodeContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyDefinitions = transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    auto pkColName =
        ctx.kU_CreateNodeConstraint() ? transformPrimaryKey(*ctx.kU_CreateNodeConstraint()) : "";
    return std::make_unique<CreateNodeTableClause>(
        std::move(schemaName), std::move(propertyDefinitions), pkColName);
}

std::unique_ptr<Statement> Transformer::transformCreateRelClause(
    CypherParser::KU_CreateRelContext& ctx) {
    auto schemaName = transformSchemaName(*ctx.oC_SchemaName(0));
    auto propertyDefinitions = ctx.kU_PropertyDefinitions() ?
                                   transformPropertyDefinitions(*ctx.kU_PropertyDefinitions()) :
                                   std::vector<std::pair<std::string, std::string>>();
    auto relMultiplicity =
        ctx.oC_SymbolicName() ? transformSymbolicName(*ctx.oC_SymbolicName()) : "MANY_MANY";
    return make_unique<CreateRelClause>(std::move(schemaName), std::move(propertyDefinitions),
        relMultiplicity, transformSchemaName(*ctx.oC_SchemaName(1)),
        transformSchemaName(*ctx.oC_SchemaName(2)));
}

std::unique_ptr<Statement> Transformer::transformDropTable(CypherParser::KU_DropTableContext& ctx) {
    return std::make_unique<DropTable>(transformSchemaName(*ctx.oC_SchemaName()));
}

std::unique_ptr<Statement> Transformer::transformRenameTable(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<RenameTable>(transformSchemaName(*ctx.oC_SchemaName()),
        transformSchemaName(*ctx.kU_AlterOptions()->kU_RenameTable()->oC_SchemaName()));
}

std::unique_ptr<Statement> Transformer::transformAddProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<AddProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_AddProperty()->oC_PropertyKeyName()),
        transformDataType(*ctx.kU_AlterOptions()->kU_AddProperty()->kU_DataType()),
        ctx.kU_AlterOptions()->kU_AddProperty()->oC_Expression() ?
            transformExpression(*ctx.kU_AlterOptions()->kU_AddProperty()->oC_Expression()) :
            std::make_unique<ParsedLiteralExpression>(
                std::make_unique<common::Value>(common::Value::createNullValue()), "NULL"));
}

std::unique_ptr<Statement> Transformer::transformDropProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<DropProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_DropProperty()->oC_PropertyKeyName()));
}

std::unique_ptr<Statement> Transformer::transformRenameProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    return std::make_unique<RenameProperty>(transformSchemaName(*ctx.oC_SchemaName()),
        transformPropertyKeyName(
            *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[0]),
        transformPropertyKeyName(
            *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[1]));
}

std::vector<std::pair<std::string, std::string>> Transformer::transformPropertyDefinitions(
    CypherParser::KU_PropertyDefinitionsContext& ctx) {
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes;
    for (auto property : ctx.kU_PropertyDefinition()) {
        propertyNameDataTypes.emplace_back(
            transformPropertyKeyName(*property->oC_PropertyKeyName()),
            transformDataType(*property->kU_DataType()));
    }
    return propertyNameDataTypes;
}

std::string Transformer::transformDataType(CypherParser::KU_DataTypeContext& ctx) {
    return ctx.getText();
}

std::string Transformer::transformListIdentifiers(CypherParser::KU_ListIdentifiersContext& ctx) {
    std::string listIdentifiers;
    for (auto& listIdentifier : ctx.kU_ListIdentifier()) {
        listIdentifiers += listIdentifier->getText();
    }
    return listIdentifiers;
}

std::string Transformer::transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

std::unique_ptr<Statement> Transformer::transformCopyCSV(CypherParser::KU_CopyCSVContext& ctx) {
    auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto parsingOptions = ctx.kU_ParsingOptions() ?
                              transformParsingOptions(*ctx.kU_ParsingOptions()) :
                              std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>();
    return std::make_unique<Copy>(std::move(filePaths), std::move(tableName),
        std::move(parsingOptions), common::CopyDescription::FileType::UNKNOWN);
}

std::unique_ptr<Statement> Transformer::transformCopyNPY(CypherParser::KU_CopyNPYContext& ctx) {
    auto filePaths = transformFilePaths(ctx.StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto parsingOptions = std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>();
    return std::make_unique<Copy>(std::move(filePaths), std::move(tableName),
        std::move(parsingOptions), common::CopyDescription::FileType::NPY);
}

std::unique_ptr<Statement> Transformer::transformCall(CypherParser::KU_CallContext& ctx) {
    auto optionName = transformSymbolicName(*ctx.oC_SymbolicName());
    auto parameter = transformLiteral(*ctx.oC_Literal());
    return std::make_unique<Call>(std::move(optionName), std::move(parameter));
}

std::vector<std::string> Transformer::transformFilePaths(
    std::vector<antlr4::tree::TerminalNode*> stringLiteral) {
    std::vector<std::string> csvFiles;
    for (auto& csvFile : stringLiteral) {
        csvFiles.push_back(transformStringLiteral(*csvFile));
    }
    return csvFiles;
}

std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>
Transformer::transformParsingOptions(CypherParser::KU_ParsingOptionsContext& ctx) {
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> copyOptions;
    for (auto loadOption : ctx.kU_ParsingOption()) {
        auto optionName = transformSymbolicName(*loadOption->oC_SymbolicName());
        copyOptions.emplace(optionName, transformLiteral(*loadOption->oC_Literal()));
    }
    return copyOptions;
}

std::string Transformer::transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral) {
    auto str = stringLiteral.getText();
    return common::StringUtils::removeEscapedCharacters(str);
}

} // namespace parser
} // namespace kuzu
