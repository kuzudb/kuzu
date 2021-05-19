#include "src/binder/include/query_binder.h"

namespace graphflow {
namespace binder {

static string makeUniqueVariableName(const string& name, uint32_t& idx) {
    return "_gf" + to_string(idx++) + "_" + name;
}

static void validateProjectionColumnNamesAreUnique(
    const vector<shared_ptr<Expression>>& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        auto k = *expression;
        if (end(existColumnNames) !=
            existColumnNames.find(expression->getAliasElseRawExpression())) {
            throw invalid_argument("Multiple result column with the same name " +
                                   expression->getAliasElseRawExpression() + " are not supported.");
        }
        existColumnNames.insert(expression->getAliasElseRawExpression());
    }
}

static void validateOnlyFunctionIsCountStar(vector<shared_ptr<Expression>>& expressions) {
    for (auto& expression : expressions) {
        if (FUNCTION == expression->expressionType &&
            FUNCTION_COUNT_STAR == expression->variableName && expressions.size() != 1) {
            throw invalid_argument("The only function in the return clause should be COUNT(*).");
        }
    }
}

static void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    const unordered_map<string, shared_ptr<Expression>>& variablesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, variable] : variablesInScope) {
        if (NODE == variable->dataType) {
            visited.insert(variable->variableName);
        }
    }
    if (visited.empty()) {
        visited.insert(queryGraph.queryNodes[0]->variableName);
    }
    auto target = visited;
    for (auto& queryNode : queryGraph.queryNodes) {
        target.insert(queryNode->variableName);
    }
    auto frontier = visited;
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = queryGraph.getNeighbourNodeNames(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr)) {
                    visited.insert(nbr);
                    nextFrontier.insert(nbr);
                }
            }
        }
        if (visited.size() == target.size()) {
            return;
        }
        frontier = nextFrontier;
    }
    throw invalid_argument("Disconnect query graph is not supported.");
}

unique_ptr<BoundSingleQuery> QueryBinder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto& parsedQueryPart : singleQuery.queryParts) {
        auto boundQueryPart = make_unique<BoundQueryPart>();
        if (!parsedQueryPart->readingStatements.empty()) {
            boundQueryPart->boundMatchStatement =
                bindMatchStatementsAndMerge(parsedQueryPart->readingStatements);
        }
        boundQueryPart->boundWithStatement = bindWithStatement(*parsedQueryPart->withStatement);
        boundSingleQuery->boundQueryParts.push_back(move(boundQueryPart));
    }
    if (!singleQuery.readingStatements.empty()) {
        boundSingleQuery->boundMatchStatement =
            bindMatchStatementsAndMerge(singleQuery.readingStatements);
    }
    boundSingleQuery->boundReturnStatement = bindReturnStatement(*singleQuery.returnStatement);
    return boundSingleQuery;
}

unique_ptr<BoundMatchStatement> QueryBinder::bindMatchStatementsAndMerge(
    const vector<unique_ptr<ReadingStatement>>& readingStatements) {
    unique_ptr<BoundMatchStatement> mergedMatchStatement;
    for (auto& matchStatement : readingStatements) {
        if (!mergedMatchStatement) {
            mergedMatchStatement = bindMatchStatement((MatchStatement&)*matchStatement);
        } else {
            mergedMatchStatement->merge(*bindMatchStatement((MatchStatement&)*matchStatement));
        }
    }
    return mergedMatchStatement;
}

unique_ptr<BoundMatchStatement> QueryBinder::bindMatchStatement(
    const MatchStatement& matchStatement) {
    auto prevVariablesInScope = variablesInScope;
    auto queryGraph = bindQueryGraph(matchStatement.graphPattern);
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchStatement = make_unique<BoundMatchStatement>(move(queryGraph));
    if (matchStatement.whereClause) {
        boundMatchStatement->whereExpression = bindWhereExpression(*matchStatement.whereClause);
    }
    return boundMatchStatement;
}

unique_ptr<BoundWithStatement> QueryBinder::bindWithStatement(const WithStatement& withStatement) {
    auto expressionsToProject =
        bindProjectExpressions(withStatement.expressions, withStatement.containsStar);
    variablesInScope.clear();
    for (auto& expression : expressionsToProject) {
        if (expression->alias.empty()) {
            throw invalid_argument("Expression in WITH multi be aliased (use AS).");
        }
        variablesInScope.insert({expression->getAliasElseRawExpression(), expression});
    }
    auto boundWithStatement = make_unique<BoundWithStatement>(move(expressionsToProject));
    if (withStatement.whereClause) {
        boundWithStatement->whereExpression = bindWhereExpression(*withStatement.whereClause);
    }
    return boundWithStatement;
}

unique_ptr<BoundReturnStatement> QueryBinder::bindReturnStatement(
    const ReturnStatement& returnStatement) {
    return make_unique<BoundReturnStatement>(
        bindProjectExpressions(returnStatement.expressions, returnStatement.containsStar));
}

shared_ptr<Expression> QueryBinder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto whereExpression = expressionBinder->bindExpression(parsedExpression);
    if (BOOL != whereExpression->dataType) {
        throw invalid_argument("Type mismatch: " + whereExpression->rawExpression + " returns " +
                               dataTypeToString(whereExpression->dataType) + " expected Boolean.");
    }
    return whereExpression;
}

vector<shared_ptr<Expression>> QueryBinder::bindProjectExpressions(
    const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto expressions = vector<shared_ptr<Expression>>();
    if (projectStar) {
        if (variablesInScope.empty()) {
            throw invalid_argument(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            expressions.push_back(expression);
        }
    }
    for (auto& parsedExpression : parsedExpressions) {
        expressions.push_back(expressionBinder->bindExpression(*parsedExpression));
    }
    validateProjectionColumnNamesAreUnique(expressions);
    validateOnlyFunctionIsCountStar(expressions);
    return expressions;
}

unique_ptr<QueryGraph> QueryBinder::bindQueryGraph(
    const vector<unique_ptr<PatternElement>>& graphPattern) {
    auto queryGraph = make_unique<QueryGraph>();
    for (auto& patternElement : graphPattern) {
        auto leftNode = bindQueryNode(*patternElement->nodePattern, *queryGraph).get();
        for (auto& patternElementChain : patternElement->patternElementChains) {
            auto rightNode = bindQueryNode(*patternElementChain->nodePattern, *queryGraph).get();
            bindQueryRel(*patternElementChain->relPattern, leftNode, rightNode, *queryGraph);
            leftNode = rightNode;
        }
    }
    return queryGraph;
}

void QueryBinder::bindQueryRel(const RelPattern& relPattern, NodeExpression* leftNode,
    NodeExpression* rightNode, QueryGraph& queryGraph) {
    auto parsedName = relPattern.name;
    if (variablesInScope.contains(parsedName)) {
        auto variableInScope = variablesInScope.at(parsedName);
        if (REL != variableInScope->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   dataTypeToString(variableInScope->dataType) +
                                   " (expect RELATIONSHIP).");
        } else {
            // Bind to queryRel in scope requires QueryRel takes multiple src & dst nodes
            // Example MATCH (a)-[r1]->(b) MATCH (c)-[r1]->(d)
            // Should be bound as (a,c)-[r1]->(b,d)
            throw invalid_argument("Bind relationship " + parsedName +
                                   " to relationship with same name is not supported.");
        }
    }
    auto queryRel = make_shared<RelExpression>(makeUniqueVariableName(parsedName, lastVariableIdx),
        bindRelLabel(relPattern.label), relPattern.lowerBound, relPattern.upperBound);
    queryRel->alias = parsedName;
    auto isLeftNodeSrc = RIGHT == relPattern.arrowDirection;
    bindNodeToRel(*queryRel, leftNode, isLeftNodeSrc);
    bindNodeToRel(*queryRel, rightNode, !isLeftNodeSrc);
    if (!queryRel->alias.empty()) {
        variablesInScope.insert({queryRel->alias, queryRel});
    }
    queryGraph.addQueryRelIfNotExist(queryRel);
}

void QueryBinder::bindNodeToRel(
    RelExpression& queryRel, NodeExpression* queryNode, bool isSrcNode) {
    if (ANY_LABEL != queryNode->label && ANY_LABEL != queryRel.label) {
        auto relLabels =
            catalog.getRelLabelsForNodeLabelDirection(queryNode->label, isSrcNode ? FWD : BWD);
        for (auto& relLabel : relLabels) {
            if (relLabel == queryRel.label) {
                isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
                return;
            }
        }
        throw invalid_argument("Node " + queryNode->getAliasElseRawExpression() +
                               " doesn't connect to edge with same type as " +
                               queryRel.getAliasElseRawExpression() + ".");
    }
    isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
}

shared_ptr<NodeExpression> QueryBinder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.name;
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto variableInScope = variablesInScope.at(parsedName);
        if (NODE != variableInScope->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   dataTypeToString(variableInScope->dataType) + " (expect NODE).");
        }
        queryNode = static_pointer_cast<NodeExpression>(variableInScope);
        auto otherLabel = bindNodeLabel(nodePattern.label);
        if (ANY_LABEL == queryNode->label) {
            queryNode->label = otherLabel;
        } else if (ANY_LABEL != otherLabel && queryNode->label != otherLabel) {
            throw invalid_argument(
                "Multi-label is not supported. Node " + parsedName + " is given multiple labels.");
        }
    } else {
        queryNode = make_shared<NodeExpression>(
            makeUniqueVariableName(parsedName, lastVariableIdx), bindNodeLabel(nodePattern.label));
        queryNode->alias = parsedName;
        if (!queryNode->alias.empty()) {
            variablesInScope.insert({queryNode->alias, queryNode});
        }
    }
    queryGraph.addQueryNodeIfNotExist(queryNode);
    return queryNode;
}

label_t QueryBinder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label.c_str())) {
        throw invalid_argument("Rel label " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label.c_str());
}

label_t QueryBinder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label.c_str())) {
        throw invalid_argument("Node label " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromString(parsed_label.c_str());
}

} // namespace binder
} // namespace graphflow
