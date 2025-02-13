#include "binder/binder.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "function/scalar_function.h"
#include "function/schema/vector_node_rel_functions.h"
#include "main/client_context.h"
#include "parser/query/reading_clause/match_clause.h"
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static void collectHintPattern(const BoundJoinHintNode& node, binder::expression_set& set) {
    if (node.isLeaf()) {
        set.insert(node.nodeOrRel);
        return;
    }
    for (auto& child : node.children) {
        collectHintPattern(*child, set);
    }
}

static void validateHintCompleteness(const BoundJoinHintNode& root, const QueryGraph& queryGraph) {
    binder::expression_set set;
    collectHintPattern(root, set);
    for (auto& nodeOrRel : queryGraph.getAllPatterns()) {
        if (nodeOrRel->getVariableName().empty()) {
            throw BinderException(
                "Cannot hint join order in a match patter with anonymous node or relationship.");
        }
        if (!set.contains(nodeOrRel)) {
            throw BinderException(
                stringFormat("Cannot find {} in join hint.", nodeOrRel->toString()));
        }
    }
}

std::unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = readingClause.constCast<MatchClause>();
    auto boundGraphPattern = bindGraphPattern(matchClause.getPatternElementsRef());
    std::shared_ptr<Expression> semanticExpression;
    auto queryGraphsNum = boundGraphPattern.queryGraphCollection.getNumQueryGraphs();
    for (uint32_t i = 0; i < queryGraphsNum; ++i){
        for(auto & expr: boundGraphPattern.queryGraphCollection.getQueryGraph(i)->getSemanticExpressions()) {
            if(!semanticExpression){
                semanticExpression = expr;
            } else {
                semanticExpression = expressionBinder.bindBooleanExpression(kuzu::common::ExpressionType::AND,
                    binder::expression_vector{semanticExpression, expr});
            }
        }
    }
    if (matchClause.hasWherePredicate() && semanticExpression) {
        boundGraphPattern.where =
            expressionBinder.bindBooleanExpression(kuzu::common::ExpressionType::AND,
                binder::expression_vector{semanticExpression,
                    bindWhereExpression(*matchClause.getWherePredicate())});
    } else if (matchClause.hasWherePredicate()) {
        boundGraphPattern.where = bindWhereExpression(*matchClause.getWherePredicate());
    } else if (semanticExpression) {
        boundGraphPattern.where = semanticExpression;
    }
    rewriteMatchPattern(boundGraphPattern);
    auto boundMatch = std::make_unique<BoundMatchClause>(
        std::move(boundGraphPattern.queryGraphCollection), matchClause.getMatchClauseType());
    if (matchClause.hasHint()) {
        if (boundMatch->getQueryGraphCollection()->getNumQueryGraphs() > 1) {
            throw BinderException("Join hint on disconnected match pattern is not supported.");
        }
        auto hint = bindJoinHint((*matchClause.getHint()));
        validateHintCompleteness(*hint, *boundMatch->getQueryGraphCollection()->getQueryGraph(0));
        boundMatch->setHint(std::move(hint));
    }
    boundMatch->setPredicate(boundGraphPattern.where);
    return boundMatch;
}

std::shared_ptr<BoundJoinHintNode> Binder::bindJoinHint(const parser::JoinHintNode& joinHintNode) {
    if (joinHintNode.isLeaf()) {
        std::shared_ptr<Expression> pattern = nullptr;
        if (scope.contains(joinHintNode.variableName)) {
            pattern = scope.getExpression(joinHintNode.variableName);
        }
        if (pattern == nullptr || pattern->expressionType != ExpressionType::PATTERN) {
            throw BinderException(stringFormat("Cannot bind {} to a node or relationship pattern",
                joinHintNode.variableName));
        }
        return std::make_shared<BoundJoinHintNode>(std::move(pattern));
    }
    auto node = std::make_shared<BoundJoinHintNode>();
    for (auto& child : joinHintNode.children) {
        node->addChild(bindJoinHint(*child));
    }
    return node;
}

void Binder::rewriteMatchPattern(BoundGraphPattern& boundGraphPattern) {
    // Rewrite self loop edge
    // e.g. rewrite (a)-[e]->(a) as [a]-[e]->(b) WHERE id(a) = id(b)
    expression_vector selfLoopEdgePredicates;
    auto& graphCollection = boundGraphPattern.queryGraphCollection;
    for (auto i = 0u; i < graphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = graphCollection.getQueryGraphUnsafe(i);
        for (auto& queryRel : queryGraph->getQueryRels()) {
            if (!queryRel->isSelfLoop()) {
                continue;
            }
            auto src = queryRel->getSrcNode();
            auto dst = queryRel->getDstNode();
            auto newDst = createQueryNode(dst->getVariableName(), dst->getEntries());
            queryGraph->addQueryNode(newDst);
            queryRel->setDstNode(newDst);
            auto predicate = expressionBinder.createEqualityComparisonExpression(
                src->getInternalID(), newDst->getInternalID());
            selfLoopEdgePredicates.push_back(std::move(predicate));
        }
    }
    auto where = boundGraphPattern.where;
    for (auto& predicate : selfLoopEdgePredicates) {
        where = expressionBinder.combineBooleanExpressions(ExpressionType::AND, predicate, where);
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto i = 0u; i < graphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = graphCollection.getQueryGraphUnsafe(i);
        for (auto& pattern : queryGraph->getAllPatterns()) {
            for (auto& [propertyName, rhs] : pattern->getPropertyDataExprRef()) {
                auto propertyExpr =
                    expressionBinder.bindNodeOrRelPropertyExpression(*pattern, propertyName);
                auto predicate =
                    expressionBinder.createEqualityComparisonExpression(propertyExpr, rhs);
                where = expressionBinder.combineBooleanExpressions(ExpressionType::AND, predicate,
                    where);
            }
        }
    }
    boundGraphPattern.where = std::move(where);
}

} // namespace binder
} // namespace kuzu
